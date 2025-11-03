import boto3
from typing import Iterable, Optional, List
from decimal import Decimal
from ..ports.fill_provider import FillProvider
from ..domain.models import Fill
from ..domain.time_window import TimeWindow

_SQL = """
SELECT wallet,
       coin,
       CASE WHEN side='A' THEN 'A' ELSE 'B' END as side,
       px,
       sz,
       EXTRACT(EPOCH FROM ts)*1000::bigint as ts_ms,
       hash,
       base_tid,
       trade_id,
       role,
       counterparty,
       notional_usd
FROM {schema}.{table}
WHERE wallet = :wallet
  AND coin NOT LIKE '@%%'
  AND ts >= (TIMESTAMP 'epoch' + (:start_ms/1000.0) * INTERVAL '1 second')
  AND ts <  (TIMESTAMP 'epoch' + (:end_ms/1000.0)   * INTERVAL '1 second')
  {coin_filter}
ORDER BY ts
"""

class RedshiftDataApiProvider(FillProvider):
    def __init__(self, workgroup_or_cluster: str, database: str, secret_arn: str,
                 schema: str, table: str):
        self._client = boto3.client("redshift-data")
        self._wg_or_cluster = workgroup_or_cluster
        self._db = database
        self._secret = secret_arn
        self._schema = schema
        self._table = table

    def fetch_fills(self, wallet: str, window: TimeWindow, coin: Optional[str]=None) -> Iterable[Fill]:
        coin_filter = "AND coin = :coin" if coin else ""
        sql = _SQL.format(schema=self._schema, table=self._table, coin_filter=coin_filter)
        params = [
            {"name":"wallet","value":{"stringValue":wallet}},
            {"name":"start_ms","value":{"longValue":window.start_ms}},
            {"name":"end_ms","value":{"longValue":window.end_ms}},
        ]
        if coin:
            params.append({"name":"coin","value":{"stringValue":coin}})
        exec_args = dict(
            Database=self._db,
            SecretArn=self._secret,
            Sql=sql,
            Parameters=params
        )
        # Serverless vs Cluster:
        # אם נתת Workgroup (Serverless) – השתמש בזה; אחרת ClusterIdentifier
        if self._wg_or_cluster and not self._wg_or_cluster.lower().startswith("cluster:"):
            exec_args["WorkgroupName"] = self._wg_or_cluster
        else:
            exec_args["ClusterIdentifier"] = self._wg_or_cluster.replace("cluster:","")

        sid = self._client.execute_statement(**exec_args)["Id"]
        desc = self._client.describe_statement(Id=sid)
        while desc["Status"] in ("SUBMITTED","PICKED","STARTED"):
            desc = self._client.describe_statement(Id=sid)
        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Redshift Data API failed: {desc}")
        rows = self._client.get_statement_result(Id=sid)["Records"]

        out: List[Fill] = []
        for r in rows:
            # הסדר תואם ל-SELECT
            wallet_v   = list(r[0].values())[0]
            coin_v     = list(r[1].values())[0]
            side_v     = list(r[2].values())[0]
            px_v       = list(r[3].values())[0]
            sz_v       = list(r[4].values())[0]
            ts_ms_v    = list(r[5].values())[0]
            hash_v     = list(r[6].values())[0] if r[6] else None
            base_tid_v = list(r[7].values())[0] if r[7] else None
            trade_id_v = list(r[8].values())[0] if r[8] else None
            role_v     = list(r[9].values())[0] if r[9] else None
            counter_v  = list(r[10].values())[0] if r[10] else None
            notional_v = list(r[11].values())[0] if r[11] else None

            out.append(Fill(
                wallet=wallet_v,
                coin=coin_v,
                side=side_v,
                px=Decimal(str(px_v)),
                sz=Decimal(str(sz_v)),
                ts_ms=int(ts_ms_v),
                hash=hash_v,
                base_tid=int(base_tid_v) if base_tid_v is not None else None,
                role=role_v,
                counterparty=counter_v,
                trade_id=int(trade_id_v) if trade_id_v is not None else None,
                notional_usd=Decimal(str(notional_v)) if notional_v is not None else None,
            ))
        return out
