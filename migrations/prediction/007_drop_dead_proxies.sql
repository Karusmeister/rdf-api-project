-- Drop the dead_proxies table.
--
-- Context: the batch worker pool's proxy rotation feature has been retired
-- (NordVPN + public proxy support removed). With proxies gone, no code path
-- writes to or reads from dead_proxies. The table is dead schema.
--
-- Dropping it here is a prerequisite for migration 008 (CREATE ROLE rdf_batch),
-- so the new role's grant list doesn't have to mention a phantom table.
--
-- Safe to run: BATCH_USE_VPN=false on the batch VM means proxy_pool.py is
-- never invoked at runtime, so DeadProxyRegistry is never instantiated and
-- this table receives no writes. The 1523 rows present at drop time are
-- expired TTL cache entries (6h TTL, last write ~24h ago).

DROP TABLE IF EXISTS dead_proxies;
