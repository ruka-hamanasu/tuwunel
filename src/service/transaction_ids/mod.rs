use std::{fmt::Write, sync::Arc};

use async_trait::async_trait;
use lru_cache::LruCache;
use ruma::{
	DeviceId, ServerName, TransactionId, UserId,
	api::federation::transactions::send_transaction_message,
};
use tokio::sync::Mutex;
use tuwunel_core::{
	Result, implement,
	utils::{
		MutexMap, MutexMapGuard,
		math::usize_from_f64,
	},
};
use tuwunel_database::{Handle, Map};

pub struct Service {
	db: Data,
	mem: Memory,
}

struct Data {
	userdevicetxnid_response: Option<Arc<Map>>,
}

struct Memory {
	federation_txnid_response: Mutex<FederationTxnidCache>,
	federation_txnid_mutex: MutexMap<Vec<u8>, ()>,
}

type FederationTxnidCache = LruCache<Vec<u8>, send_transaction_message::v1::Response>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let cache_size = f64::from(config.federation_inbound_txnid_cache_capacity);
		let cache_size = cache_size * config.cache_capacity_modifier;

		Ok(Arc::new(Self {
			db: Data {
				userdevicetxnid_response: Some(args.db["userdevicetxnid_response"].clone()),
			},
			mem: Memory {
				federation_txnid_response: Mutex::new(FederationTxnidCache::new(usize_from_f64(
					cache_size,
				)?)),
				federation_txnid_mutex: MutexMap::new(),
			},
		}))
	}

	async fn clear_cache(&self) {
		self.mem
			.federation_txnid_response
			.lock()
			.await
			.clear();
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let cache = self.mem.federation_txnid_response.lock().await;
		writeln!(out, "federation_txnid_cache: {}/{}", cache.len(), cache.capacity())?;

		Ok(())
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

fn federation_txnid_key(origin: &ServerName, txn_id: &TransactionId) -> Vec<u8> {
	let mut key = origin.as_bytes().to_vec();
	key.push(0xFF);
	key.extend_from_slice(txn_id.as_bytes());

	key
}

#[implement(Service)]
pub fn add_txnid(
	&self,
	user_id: &UserId,
	device_id: Option<&DeviceId>,
	txn_id: &TransactionId,
	data: &[u8],
) {
	let mut key = user_id.as_bytes().to_vec();
	key.push(0xFF);
	key.extend_from_slice(
		device_id
			.map(DeviceId::as_bytes)
			.unwrap_or_default(),
	);
	key.push(0xFF);
	key.extend_from_slice(txn_id.as_bytes());

	self.db
		.userdevicetxnid_response
		.as_ref()
		.expect("userdevicetxnid_response map is initialized")
		.insert(&key, data);
}

// If there's no entry, this is a new transaction
#[implement(Service)]
pub async fn existing_txnid(
	&self,
	user_id: &UserId,
	device_id: Option<&DeviceId>,
	txn_id: &TransactionId,
) -> Result<Handle<'_>> {
	let key = (user_id, device_id, txn_id);
	self
		.db
		.userdevicetxnid_response
		.as_ref()
		.expect("userdevicetxnid_response map is initialized")
		.qry(&key)
		.await
}

#[implement(Service)]
pub async fn lock_federation_txnid(
	&self,
	origin: &ServerName,
	txn_id: &TransactionId,
) -> MutexMapGuard<Vec<u8>, ()> {
	let key = federation_txnid_key(origin, txn_id);
	self.mem.federation_txnid_mutex.lock(&key).await
}

#[implement(Service)]
pub async fn existing_federation_txnid(
	&self,
	origin: &ServerName,
	txn_id: &TransactionId,
) -> Option<send_transaction_message::v1::Response> {
	let key = federation_txnid_key(origin, txn_id);
	self.mem
		.federation_txnid_response
		.lock()
		.await
		.get_mut(&key)
		.cloned()
}

#[implement(Service)]
pub async fn add_federation_txnid(
	&self,
	origin: &ServerName,
	txn_id: &TransactionId,
	response: send_transaction_message::v1::Response,
) {
	let key = federation_txnid_key(origin, txn_id);
	self.mem
		.federation_txnid_response
		.lock()
		.await
		.insert(key, response);
}

#[cfg(test)]
impl Service {
	fn with_federation_cache_capacity(capacity: usize) -> Self {
		Self {
			db: Data {
				userdevicetxnid_response: None,
			},
			mem: Memory {
				federation_txnid_response: Mutex::new(FederationTxnidCache::new(capacity)),
				federation_txnid_mutex: MutexMap::new(),
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{sync::Arc, time::Duration};

	use ruma::{
		OwnedServerName, OwnedTransactionId,
		api::federation::transactions::send_transaction_message,
	};
	use tokio::{task::JoinHandle, time::timeout};

	fn origin(server: &str) -> OwnedServerName {
		server
			.try_into()
			.expect("valid server name")
	}

	fn txn_id(txn: &str) -> OwnedTransactionId {
		txn.try_into().expect("valid transaction id")
	}

	fn empty_response() -> send_transaction_message::v1::Response {
		send_transaction_message::v1::Response {
			pdus: Default::default(),
		}
	}

	#[tokio::test]
	async fn federation_txnid_cache_add_get_and_eviction() {
		let service = Service::with_federation_cache_capacity(2);
		let origin = origin("example.com");
		let txn_a = txn_id("txn-a");
		let txn_b = txn_id("txn-b");
		let txn_c = txn_id("txn-c");

		service
			.add_federation_txnid(&origin, &txn_a, empty_response())
			.await;
		service
			.add_federation_txnid(&origin, &txn_b, empty_response())
			.await;

		assert!(service
			.existing_federation_txnid(&origin, &txn_a)
			.await
			.is_some());
		assert!(service
			.existing_federation_txnid(&origin, &txn_b)
			.await
			.is_some());

		service
			.add_federation_txnid(&origin, &txn_c, empty_response())
			.await;

		assert!(service
			.existing_federation_txnid(&origin, &txn_c)
			.await
			.is_some());
		assert!(service
			.existing_federation_txnid(&origin, &txn_b)
			.await
			.is_some());
		assert!(service
			.existing_federation_txnid(&origin, &txn_a)
			.await
			.is_none());
	}

	#[tokio::test]
	async fn federation_txnid_lock_serializes_same_key() {
		let service = Arc::new(Service::with_federation_cache_capacity(4));
		let origin = origin("example.com");
		let txn = txn_id("txn-lock");

		let first_guard = service
			.lock_federation_txnid(&origin, &txn)
			.await;

		let cloned = Arc::clone(&service);
		let origin2 = origin.clone();
		let txn2 = txn.clone();
		let waiter: JoinHandle<()> = tokio::spawn(async move {
			let _second_guard = cloned
				.lock_federation_txnid(&origin2, &txn2)
				.await;
		});

		assert!(timeout(Duration::from_millis(50), async {
			while !waiter.is_finished() {
				tokio::task::yield_now().await;
			}
		})
		.await
		.is_err());

		drop(first_guard);

		timeout(Duration::from_secs(1), waiter)
			.await
			.expect("second waiter completed")
			.expect("second waiter succeeded");
	}

	#[tokio::test]
	async fn federation_txnid_cache_is_cleared_by_service_clear_cache() {
		let service = Service::with_federation_cache_capacity(8);
		let origin = origin("example.com");
		let txn = txn_id("txn-clear");

		service
			.add_federation_txnid(&origin, &txn, empty_response())
			.await;
		assert!(service
			.existing_federation_txnid(&origin, &txn)
			.await
			.is_some());

		<Service as crate::Service>::clear_cache(&service).await;

		assert!(service
			.existing_federation_txnid(&origin, &txn)
			.await
			.is_none());
	}

	#[tokio::test]
	async fn federation_txnid_cache_memory_usage_reports_len_and_capacity() {
		let service = Service::with_federation_cache_capacity(3);
		let origin = origin("example.com");
		let txn = txn_id("txn-memory");

		service
			.add_federation_txnid(&origin, &txn, empty_response())
			.await;

		let mut output = String::new();
		<Service as crate::Service>::memory_usage(&service, &mut output)
			.await
			.expect("memory usage succeeds");

		assert!(output.contains("federation_txnid_cache: 1/3"));
	}
}
