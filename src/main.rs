use aws_sdk_kinesis as kinesis;

#[tokio::main]
async fn main() -> Result<(), kinesis::Error> {

    let version = env!("CARGO_PKG_VERSION");

    let quiet = std::env::args().any(|arg| arg == "--quiet" || arg == "-q");
    
    if !quiet {
        // Clear terminal
        print!("{}[2J", 27 as char);
        // Set cursor to top left
        print!("{}[1;1H", 27 as char);
        println!("AWS Kinesis Event Viewer v{}", version);
        println!("=========================================");
    }

    // Check if all AWS envinroment variables are set
    let aws_env_vars = vec!["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "AWS_KINESIS_STREAM_NAME"];
    for env_var in aws_env_vars {
        if std::env::var(env_var).is_err() {
            println!("{} is not set", env_var);
            return Ok(());
        }
    }

    let aws_access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
    let aws_secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
    let aws_default_region = std::env::var("AWS_DEFAULT_REGION").unwrap();
    let aws_kinesis_stream_name = std::env::var("AWS_KINESIS_STREAM_NAME").unwrap();

    // Print out credentials
    if !quiet {
        println!("AWS_ACCESS_KEY_ID: {}", aws_access_key_id);
        println!("AWS_SECRET_ACCESS_KEY: {}", aws_secret_access_key);
        println!("AWS_DEFAULT_REGION: {}", aws_default_region);
        println!("AWS_KINESIS_STREAM_NAME: {}", aws_kinesis_stream_name);
        println!("=========================================");
    }

    // Build client
    let config = aws_config::load_from_env().await;
    let client = kinesis::Client::new(&config);

    // Get stream
    let stream_resp = client.describe_stream().set_stream_name(Some(aws_kinesis_stream_name.to_owned())).send().await?;
    let stream_description = stream_resp.stream_description().unwrap();

    // Exit if there are no active shards
    if stream_description.shards().unwrap_or_default().is_empty() {
        println!("No shards found for stream {}", aws_kinesis_stream_name);
        return Ok(());
    }

    // Get first shard id
    let first_shard_id = stream_description.shards().unwrap()[0].shard_id().unwrap();
    if !quiet {
        println!("Shard ID: {}", first_shard_id);
    }

    // Get shard iterator
    let shard_iterator_resp = 
        client.get_shard_iterator()
              .set_stream_name(Some(aws_kinesis_stream_name))
              .set_shard_id(Some(first_shard_id.to_owned()))
              .set_shard_iterator_type(Some(kinesis::model::ShardIteratorType::Latest)).send().await?;

    let shard_iterator = shard_iterator_resp.shard_iterator().unwrap();
    if !quiet {
        println!("=========================================");
    }

    // Loop data
    let mut next_shard_iterator = shard_iterator.to_owned();
    loop {
        let get_records_resp = client.get_records().set_shard_iterator(Some(next_shard_iterator)).send().await?;
        let records = get_records_resp.records().unwrap();
        for record in records {
            let data = record.data().unwrap();
            let data_str = String::from_utf8_lossy(data.as_ref());
            println!("{}\n", data_str);
        }
        next_shard_iterator = get_records_resp.next_shard_iterator().unwrap().to_owned();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    //Ok(())

}
