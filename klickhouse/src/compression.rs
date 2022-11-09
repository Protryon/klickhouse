use crate::block::Block;
use crate::{KlickhouseError, Result};

pub async fn compress_block(block: Block, revision: u64) -> Result<(Vec<u8>, usize)> {
    let mut raw = vec![];
    block.write(&mut raw, revision).await?;
    // print!("raw block out: ");
    // for b in &raw {
    //     print!("{b:02X}");
    // }
    // println!();
    let raw_len = raw.len();
    let mut compressed = Vec::<u8>::with_capacity(raw.len() + (raw.len() / 255) + 16 + 1);
    let out_len = unsafe {
        lz4::liblz4::LZ4_compress_default(
            raw.as_ptr() as *const i8,
            compressed.as_mut_ptr() as *mut i8,
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

pub async fn decompress_block(data: &[u8], decompressed_size: u32, revision: u64) -> Result<Block> {
    let mut output = Vec::with_capacity(decompressed_size as usize + 1);

    let out_len = unsafe {
        lz4::liblz4::LZ4_decompress_safe(
            data.as_ptr() as *const i8,
            output.as_mut_ptr() as *mut i8,
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

    let block = Block::read(&mut &output[..], revision).await?;

    Ok(block)
}
