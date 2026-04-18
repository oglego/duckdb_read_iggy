use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use iggy::prelude::{Client, Consumer, IggyClient, Identifier, MessageClient, PollingStrategy, UserClient};
use once_cell::sync::Lazy;
use std::sync::RwLock;
use std::error::Error;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

#[repr(C)]
struct IggyBindData {
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: u32,
    server_address: String, // New field for connection string
}

struct IggyInitData {
    client: IggyClient,
    current_offset: RwLock<u64>,
    finished: RwLock<bool>,
}

struct IggyVTab;

impl VTab for IggyVTab {
    type InitData = IggyInitData;
    type BindData = IggyBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("offset", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("payload", LogicalTypeHandle::from(LogicalTypeId::Blob));

        // extract parameters: read_iggy(stream, topic, partition, address)
        let stream_id = Identifier::from_str_value(&bind.get_parameter(0).to_string())?;
        let topic_id = Identifier::from_str_value(&bind.get_parameter(1).to_string())?;
        let partition_id = bind.get_parameter(2).to_string().parse::<u32>().unwrap_or(1);
        let server_address = bind.get_parameter(3).to_string();

        Ok(IggyBindData {
            stream_id,
            topic_id,
            partition_id,
            server_address,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        println!("Initializing Iggy Client...");
        let bind_data = info.get_bind_data::<IggyBindData>();
        let address = unsafe { (*bind_data).server_address.clone() };

        // Pass the address to the Iggy Client
        let client = RUNTIME.block_on(async {
            // Using the Iggy SDK to connect to a specific TCP address
            let client = IggyClient::from_connection_string(&address)?;
            client.connect().await?;
            client.login_user("iggy", "iggy").await?; 
            Ok::<IggyClient, Box<dyn Error>>(client)
        })?;

        println!("Connected to Iggy at {}", address);
        Ok(IggyInitData {
            client,
            current_offset: RwLock::new(0),
            finished: RwLock::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Polling Iggy for new messages...");
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if *init_data.finished.read().unwrap() {
            output.set_len(0);
            return Ok(());
        }

        let messages = RUNTIME.block_on(async {
            let current_offset = *init_data.current_offset.read().unwrap();
            let (stream_id, topic_id, partition_id) = unsafe {
                ((*bind_data).stream_id.clone(), (*bind_data).topic_id.clone(), (*bind_data).partition_id)
            };
            init_data.client.poll_messages(
                &stream_id,
                &topic_id,
                Some(partition_id),
                &Consumer::default(),
                &PollingStrategy::offset(current_offset),
                1024,
                false,
            ).await
        })?;

        if messages.messages.is_empty() {
            *init_data.finished.write().unwrap() = true;
            output.set_len(0);
            return Ok(());
        }

        let offset_vec = output.flat_vector(0);
        let payload_vec = output.flat_vector(1);

        let count = messages.messages.len();
        for (i, msg) in messages.messages.iter().enumerate() {
            unsafe {
                let offset_data = offset_vec.as_mut_ptr() as *mut i64;
                *offset_data.add(i) = msg.header.offset as i64;
            }
            payload_vec.insert(i, msg.payload.as_ref());
            *init_data.current_offset.write().unwrap() = msg.header.offset + 1;
        }
        println!("Fetched {} messages from Iggy", count);

        output.set_len(count);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // stream
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // topic
            LogicalTypeHandle::from(LogicalTypeId::Integer), // partition
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // server_address (NEW)
        ])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<IggyVTab>("read_iggy")
        .expect("Failed to register Iggy consumer");
    Ok(())
}