use std::{
	fmt::Write,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use async_trait::async_trait;
use lru_cache::LruCache;
use ruma::{
	DeviceId, ServerName, TransactionId, UserId,
	api::federation::transactions::send_transaction_message,
};
use tokio::sync::Mutex;
use tuwunel_core::{
	Result, implement,
	utils::{MutexMap, MutexMapGuard, math::usize_from_f64},
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
	federation_txnid_lookups: AtomicU64,
	federation_txnid_hits: AtomicU64,
	federation_txnid_misses: AtomicU64,
	federation_txnid_inserts: AtomicU64,
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
				federation_txnid_lookups: AtomicU64::new(0),
				federation_txnid_hits: AtomicU64::new(0),
				federation_txnid_misses: AtomicU64::new(0),
				federation_txnid_inserts: AtomicU64::new(0),
			},
		}))
	}

	async fn clear_cache(&self) {
		self.mem
			.federation_txnid_response
			.lock()
			.await
			.clear();
		self.mem
			.federation_txnid_lookups
			.store(0, Ordering::Relaxed);
		self.mem
			.federation_txnid_hits
			.store(0, Ordering::Relaxed);
		self.mem
			.federation_txnid_misses
			.store(0, Ordering::Relaxed);
		self.mem
			.federation_txnid_inserts
			.store(0, Ordering::Relaxed);
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let cache = self.mem.federation_txnid_response.lock().await;
		writeln!(out, "federation_txnid_cache: {}/{}", cache.len(), cache.capacity())?;

		Ok(())
	}

	async fn cache_stats(&self, out: &mut (dyn Write + Send)) -> Result {
		let lookups = self
			.mem
			.federation_txnid_lookups
			.load(Ordering::Relaxed);
		let hits = self
			.mem
			.federation_txnid_hits
			.load(Ordering::Relaxed);
		let misses = self
			.mem
			.federation_txnid_misses
			.load(Ordering::Relaxed);
		let inserts = self
			.mem
			.federation_txnid_inserts
			.load(Ordering::Relaxed);
		let hit_ratio = if lookups == 0 {
			0.0
		} else {
			(hits as f64 / lookups as f64) * 100.0
		};

		writeln!(
			out,
			"federation_txnid_cache_stats: lookups={lookups} hits={hits} misses={misses} inserts={inserts} hit_ratio={hit_ratio:.1}%"
		)?;

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
	self.db
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
		.federation_txnid_lookups
		.fetch_add(1, Ordering::Relaxed);

	let hit = self
		.mem
		.federation_txnid_response
		.lock()
		.await
		.get_mut(&key)
		.cloned();

	if hit.is_some() {
		self.mem
			.federation_txnid_hits
			.fetch_add(1, Ordering::Relaxed);
	} else {
		self.mem
			.federation_txnid_misses
			.fetch_add(1, Ordering::Relaxed);
	}

	hit
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
	self.mem
		.federation_txnid_inserts
		.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
impl Service {
	fn with_federation_cache_capacity(capacity: usize) -> Self {
		Self {
			db: Data { userdevicetxnid_response: None },
			mem: Memory {
				federation_txnid_response: Mutex::new(FederationTxnidCache::new(capacity)),
				federation_txnid_mutex: MutexMap::new(),
				federation_txnid_lookups: AtomicU64::new(0),
				federation_txnid_hits: AtomicU64::new(0),
				federation_txnid_misses: AtomicU64::new(0),
				federation_txnid_inserts: AtomicU64::new(0),
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
		server.try_into().expect("valid server name")
	}

	fn txn_id(txn: &str) -> OwnedTransactionId {
		txn.try_into().expect("valid transaction id")
	}

	fn empty_response() -> send_transaction_message::v1::Response {
		send_transaction_message::v1::Response { pdus: Default::default() }
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

		assert!(
			service
				.existing_federation_txnid(&origin, &txn_a)
				.await
				.is_some()
		);
		assert!(
			service
				.existing_federation_txnid(&origin, &txn_b)
				.await
				.is_some()
		);

		service
			.add_federation_txnid(&origin, &txn_c, empty_response())
			.await;

		assert!(
			service
				.existing_federation_txnid(&origin, &txn_c)
				.await
				.is_some()
		);
		assert!(
			service
				.existing_federation_txnid(&origin, &txn_b)
				.await
				.is_some()
		);
		assert!(
			service
				.existing_federation_txnid(&origin, &txn_a)
				.await
				.is_none()
		);
	}

	#[tokio::test]
	async fn federation_txnid_lock_serializes_same_key() {
		let service = Arc::new(Service::with_federation_cache_capacity(4));
		let origin = origin("example.com");
		let txn = txn_id("txn-lock");

		let first_guard = service.lock_federation_txnid(&origin, &txn).await;

		let cloned = Arc::clone(&service);
		let origin2 = origin.clone();
		let txn2 = txn.clone();
		let waiter: JoinHandle<()> = tokio::spawn(async move {
			let _second_guard = cloned
				.lock_federation_txnid(&origin2, &txn2)
				.await;
		});

		assert!(
			timeout(Duration::from_millis(50), async {
				while !waiter.is_finished() {
					tokio::task::yield_now().await;
				}
			})
			.await
			.is_err()
		);

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
		assert!(
			service
				.existing_federation_txnid(&origin, &txn)
				.await
				.is_some()
		);

		<Service as crate::Service>::clear_cache(&service).await;

		assert!(
			service
				.existing_federation_txnid(&origin, &txn)
				.await
				.is_none()
		);
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

	#[tokio::test]
	async fn federation_txnid_cache_stats_report_hits_and_misses() {
		let service = Service::with_federation_cache_capacity(4);
		let origin = origin("example.com");
		let hit_txn = txn_id("txn-hit");
		let miss_txn = txn_id("txn-miss");

		service
			.add_federation_txnid(&origin, &hit_txn, empty_response())
			.await;

		assert!(
			service
				.existing_federation_txnid(&origin, &hit_txn)
				.await
				.is_some()
		);
		assert!(
			service
				.existing_federation_txnid(&origin, &miss_txn)
				.await
				.is_none()
		);

		let mut output = String::new();
		<Service as crate::Service>::cache_stats(&service, &mut output)
			.await
			.expect("cache stats succeeds");

		assert!(output.contains("lookups=2"));
		assert!(output.contains("hits=1"));
		assert!(output.contains("misses=1"));
		assert!(output.contains("inserts=1"));
		assert!(output.contains("hit_ratio=50.0%"));
	}

	#[tokio::test]
	async fn federation_txnid_cache_stats_reset_on_clear_cache() {
		let service = Service::with_federation_cache_capacity(4);
		let origin = origin("example.com");
		let txn = txn_id("txn-reset");

		service
			.add_federation_txnid(&origin, &txn, empty_response())
			.await;
		let _ = service
			.existing_federation_txnid(&origin, &txn)
			.await;

		<Service as crate::Service>::clear_cache(&service).await;

		let mut output = String::new();
		<Service as crate::Service>::cache_stats(&service, &mut output)
			.await
			.expect("cache stats succeeds");

		assert!(output.contains("lookups=0"));
		assert!(output.contains("hits=0"));
		assert!(output.contains("misses=0"));
		assert!(output.contains("inserts=0"));
		assert!(output.contains("hit_ratio=0.0%"));
	}
}
