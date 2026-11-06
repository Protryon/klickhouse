use std::ffi::c_char;
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use crate::block::Block;
use crate::internal_client_in::MAX_COMPRESSION_SIZE;
use crate::io::ClickhouseRead;
use crate::protocol::CompressionMethod;
use crate::{KlickhouseError, Result};

pub async fn lz4_compress_block(block: Block, revision: u64) -> Result<(Vec<u8>, usize)> {
    let mut raw = Vec::new();
    block.write(&mut raw, revision).await?;
    let raw_len = raw.len();
    let mut compressed = Vec::<u8>::with_capacity(raw.len() + (raw.len() / 255) + 16 + 1);
    let out_len = unsafe {
        lz4::liblz4::LZ4_compress_default(
            raw.as_ptr() as *const c_char,
            compressed.as_mut_ptr() as *mut c_char,
            raw.len() as i32,
            compressed.capacity() as i32,
        )
    };
    if out_len <= 0 {
        return Err(KlickhouseError::ProtocolError(
            "invalid compression state".to_string(),
        ));
    }
    if out_len as usize > compressed.capacity() {
        panic!("buffer overflow in compress_block?");
    }
    unsafe { compressed.set_len(out_len as usize) };

    Ok((compressed, raw_len))
}

pub fn lz4_decompress_block(data: &[u8], decompressed_size: u32) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(decompressed_size as usize + 1);

    let out_len = unsafe {
        lz4::liblz4::LZ4_decompress_safe(
            data.as_ptr() as *const c_char,
            output.as_mut_ptr() as *mut c_char,
            data.len() as i32,
            output.capacity() as i32,
        )
    };
    if out_len < 0 {
        return Err(KlickhouseError::ProtocolError(
            "malformed compressed block".to_string(),
        ));
    }
    if out_len as usize > output.capacity() {
        panic!("buffer overflow in decompress_block?");
    }
    unsafe { output.set_len(out_len as usize) };

    Ok(output)
}

pub async fn zstd_compress_block(block: Block, revision: u64) -> Result<(Vec<u8>, usize)> {
    let mut raw = Vec::new();
    block.write(&mut raw, revision).await?;
    let raw_len = raw.len();

    let compression_level = 1;

    match zstd::bulk::compress(&raw, compression_level) {
        Ok(compressed) => Ok((compressed, raw_len)),
        Err(e) => Err(KlickhouseError::ProtocolError(format!(
            "ZSTD compression failed: {e}"
        ))),
    }
}

pub fn zstd_decompress_block(data: &[u8], decompressed_size: u32) -> Result<Vec<u8>> {
    let mut output = vec![0u8; decompressed_size as usize];

    match zstd::bulk::decompress_to_buffer(data, &mut output) {
        Ok(out_len) => {
            if out_len != decompressed_size as usize {
                return Err(KlickhouseError::ProtocolError(format!(
                    "ZSTD decompression size mismatch: expected {decompressed_size}, got {out_len}"
                )));
            }
            Ok(output)
        }
        Err(e) => Err(KlickhouseError::ProtocolError(format!(
            "malformed ZSTD compressed block: {e}"
        ))),
    }
}

async fn read_compressed_blob(reader: &mut impl ClickhouseRead) -> Result<Vec<u8>> {
    let checksum_high = reader.read_u64_le().await? as u128;
    let checksum_low = reader.read_u64_le().await? as u128;
    let checksum = (checksum_high << 64) | checksum_low;

    let type_byte = reader.read_u8().await?;
    let Ok(compression) = CompressionMethod::try_from(type_byte) else {
        return Err(KlickhouseError::ProtocolError(format!(
            "unexpected compression algorithm identifier: '{type_byte:02X}'",
        )));
    };

    let compressed_size = reader.read_u32_le().await?;
    if compressed_size > MAX_COMPRESSION_SIZE {
        // 1 GB
        return Err(KlickhouseError::ProtocolError(format!(
            "compressed payload too large! {compressed_size} > {MAX_COMPRESSION_SIZE}"
        )));
    } else if compressed_size < 9 {
        return Err(KlickhouseError::ProtocolError(format!(
            "compressed payload too small! {compressed_size} < 9"
        )));
    }

    let decompressed_size = reader.read_u32_le().await?;

    let mut block = vec![0u8; compressed_size as usize];

    block[0] = type_byte;
    block[1..5].copy_from_slice(&compressed_size.to_le_bytes());
    block[5..9].copy_from_slice(&decompressed_size.to_le_bytes());

    reader.read_exact(&mut block[9..]).await?;

    let calc_checksum = cityhash_rs::cityhash_102_128(&block[..]);
    if calc_checksum != checksum {
        return Err(KlickhouseError::ProtocolError(format!(
            "corrupt checksum from clickhouse '{calc_checksum:032X}' vs '{checksum:032X}'"
        )));
    }

    let compressed_payload = &block[9..];

    let raw_block = match compression {
        CompressionMethod::LZ4 => {
            crate::compression::lz4_decompress_block(compressed_payload, decompressed_size)?
        }
        CompressionMethod::ZSTD => {
            crate::compression::zstd_decompress_block(compressed_payload, decompressed_size)?
        }
        _ => {
            return Err(KlickhouseError::ProtocolError(format!(
                "Compression method {compression:?} is not implemented yet for byte {type_byte}"
            )));
        }
    };

    Ok(raw_block)
}

type BlockReadingFuture<R> =
    Pin<Box<dyn Future<Output = Result<(Vec<u8>, &'static mut R)>> + Send + Sync>>;

pub struct DecompressionReader<'a, R: ClickhouseRead + 'static> {
    inner: Option<&'a mut R>,
    decompressed: Vec<u8>,
    position: usize,
    block_reading_future: Option<BlockReadingFuture<R>>,
}

impl<'a, R: ClickhouseRead + 'static> DecompressionReader<'a, R> {
    pub fn new(inner: &'a mut R) -> Self {
        Self {
            inner: Some(inner),
            decompressed: vec![],
            position: 0,
            block_reading_future: None,
        }
    }

    fn run_decompression(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(block_reading_future) = self.block_reading_future.as_mut() {
            match block_reading_future.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok((value, inner))) => {
                    self.block_reading_future.take();
                    self.decompressed = value;
                    assert!(self.inner.is_none());
                    self.inner = Some(inner);
                    self.position = 0;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    self.block_reading_future.take();
                    Poll::Ready(Err(std::io::Error::new(ErrorKind::UnexpectedEof, e)))
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<R: ClickhouseRead + 'static> AsyncRead for DecompressionReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.capacity() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.run_decompression(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            _ => (),
        }
        if self.inner.is_none() {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "read after EOF",
            )));
        }

        while self.position >= self.decompressed.len() {
            let static_inner: &'static mut R =
                unsafe { std::mem::transmute(self.inner.take().unwrap()) };
            self.block_reading_future = Some(Box::pin(async move {
                let value = read_compressed_blob(static_inner).await?;
                Ok((value, static_inner))
            }));
            match self.run_decompression(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                _ => (),
            }
        }
        let length = (self.decompressed.len() - self.position).min(buf.remaining());
        buf.put_slice(&self.decompressed[self.position..self.position + length]);
        self.position += length;
        Poll::Ready(Ok(()))
    }
}
