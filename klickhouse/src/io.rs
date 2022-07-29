use crate::{KlickhouseError, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::MAX_STRING_SIZE;

#[async_trait::async_trait]
pub trait ClickhouseRead: AsyncRead + Unpin + Send + Sync {
    async fn read_var_uint(&mut self) -> Result<u64>;

    async fn read_string(&mut self) -> Result<String>;

    async fn read_binary(&mut self) -> Result<Vec<u8>>;
}

#[async_trait::async_trait]
impl<T: AsyncRead + Unpin + Send + Sync> ClickhouseRead for T {
    async fn read_var_uint(&mut self) -> Result<u64> {
        let mut out = 0u64;
        for i in 0..9u64 {
            let mut octet = [0u8];
            self.read_exact(&mut octet[..]).await?;
            out |= ((octet[0] & 0x7F) as u64) << (7 * i);
            if (octet[0] & 0x80) == 0 {
                break;
            }
        }
        Ok(out)
    }

    async fn read_string(&mut self) -> Result<String> {
        let len = self.read_var_uint().await?;
        if len as usize > MAX_STRING_SIZE {
            return Err(KlickhouseError::ProtocolError(format!(
                "string too large: {} > {}",
                len, MAX_STRING_SIZE
            )));
        }
        let mut buf = Vec::with_capacity(len as usize);

        let buf_mut = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len as usize) };
        self.read_exact(buf_mut).await?;
        unsafe { buf.set_len(len as usize) };

        Ok(String::from_utf8(buf)?)
    }

    async fn read_binary(&mut self) -> Result<Vec<u8>> {
        let len = self.read_var_uint().await?;
        if len as usize > MAX_STRING_SIZE {
            return Err(KlickhouseError::ProtocolError(format!(
                "binary blob too large: {} > {}",
                len, MAX_STRING_SIZE
            )));
        }
        let mut buf = Vec::with_capacity(len as usize);

        let buf_mut = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len as usize) };
        self.read_exact(buf_mut).await?;
        unsafe { buf.set_len(len as usize) };

        self.read_exact(&mut buf[..]).await?;

        Ok(buf)
    }
}

#[async_trait::async_trait]
pub trait ClickhouseWrite: AsyncWrite + Unpin + Send + Sync + 'static {
    async fn write_var_uint(&mut self, value: u64) -> Result<()>;

    async fn write_string(&mut self, mut value: &str) -> Result<()>;
}

#[async_trait::async_trait]
impl<T: AsyncWrite + Unpin + Send + Sync + 'static> ClickhouseWrite for T {
    async fn write_var_uint(&mut self, mut value: u64) -> Result<()> {
        for _ in 0..9u64 {
            let mut byte = value & 0x7F;
            if value > 0x7F {
                byte |= 0x80;
            }
            self.write_all(&[byte as u8]).await?;
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn write_string(&mut self, value: &str) -> Result<()> {
        self.write_var_uint(value.len() as u64).await?;
        self.write_all(value.as_bytes()).await?;
        Ok(())
    }
}
