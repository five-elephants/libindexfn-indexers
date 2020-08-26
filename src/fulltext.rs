use libindexfn::{AccessStorage, IndexingError, IndexingResult, ObjectNameBuf};
use log::info;
use std::str;

pub async fn multi_index_by_words<S: AccessStorage + Sync>(
    sto: S,
    name_buf: ObjectNameBuf,
) -> IndexingResult<Vec<String>> {
    info!("Indexing words in {}", name_buf.name().as_str());

    let data = sto.read_bytes(name_buf.name()).await.map_err(|err| {
        IndexingError::new(format!(
            "Could not read object '{}': {}",
            name_buf.name().as_str(),
            err
        ))
    })?;

    let text = str::from_utf8(&data).map_err(|err| {
        IndexingError::new(format!(
            "Could not parse '{}' as UTF-8 text: {}",
            name_buf.name().as_str(),
            err
        ))
    })?;
    let lower_text = text.to_lowercase();
    let filtered_text: String = lower_text
        .chars()
        .filter(|c| c.is_alphabetic() || c.is_whitespace())
        .collect();

    let words: Vec<_> = filtered_text
        .split(char::is_whitespace)
        .filter_map(|s| {
            if s.len() > 1 {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();

    Ok(words)
}

#[cfg(test)]
mod test {
    use temp_testdir::TempDir;
    use tokio_test::block_on;

    use super::*;
    use libindexfn::{FileStorage, HashTableIndexer, Lookup, MultiIndex, ObjectName};

    #[test]
    fn test_indexing() {
        const TEXT: &str = r#"
            This is a sentence. It includes punctuation, too.
            And newlines.
        "#;

        let dir = TempDir::default();
        let sto = FileStorage::new(dir.as_ref());

        block_on(async {
            // prepare data on disk
            let foo = ObjectName::new("foo").unwrap();
            sto.write_bytes(foo, TEXT.as_bytes()).await.unwrap();

            // index data
            let words_index =
                HashTableIndexer::multi_index(&sto, ObjectName::empty(), multi_index_by_words)
                    .await
                    .unwrap();

            // test result
            assert!(!words_index.get(&String::from("this")).unwrap().is_empty());
            assert!(!words_index.get(&String::from("it")).unwrap().is_empty());
            assert!(!words_index
                .get(&String::from("punctuation"))
                .unwrap()
                .is_empty());
            assert!(!words_index.get(&String::from("newlines")).unwrap().is_empty());
        });
    }
}
