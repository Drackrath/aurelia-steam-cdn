// Modified by Aurelia; see README.
use std::io::Cursor;
use zip::ZipArchive;

use crate::{
    crypto::aes256::{self, IV_LENGTH},
    utils::lzma,
    Error,
};

/// Decrypt, decompress; cap at `max_len`.
pub async fn decrypt_and_decompress(
    data: &mut [u8],
    key: [u8; 32],
    max_len: usize,
) -> Result<Vec<u8>, Error> {
    if max_len > lzma::MAX_CHUNK_LEN {
        return Err(Error::Unexpected(format!(
            "chunk of {max_len} bytes exceeds {} byte ceiling",
            lzma::MAX_CHUNK_LEN
        )));
    }
    if data.len() <= IV_LENGTH {
        return Err(Error::Eof("data is too small".to_string()));
    }

    let decrypted = aes256::decrypt_cbc_with_iv_extraction(data, key)?;
    if lzma::is_vz(&decrypted) {
        // Legacy LZMA container.
        Ok(lzma::decompress(&decrypted, max_len).await?)
    } else if lzma::is_vsz(&decrypted) {
        // Newer Zstandard container.
        Ok(lzma::decompress_vsz(&decrypted, max_len)?)
    } else {
        // Otherwise the payload is a single-entry zip.
        let cursor = Cursor::new(decrypted);
        let mut archive = ZipArchive::new(cursor)?;
        let entry = archive.by_index(0)?;
        lzma::read_bounded(entry, max_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn over_ceiling_size_rejected_before_work() {
        let mut data = vec![0u8; 64];
        let err = decrypt_and_decompress(&mut data, [0u8; 32], lzma::MAX_CHUNK_LEN + 1)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unexpected(_)), "{err}");
    }
}
