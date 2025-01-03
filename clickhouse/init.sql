CREATE FUNCTION IF NOT EXISTS getPrefix AS (etype, addr) ->
(
    multiIf(
        etype = 0x0800, dictGetOrDefault('networks_dict', 'prefix', toIPv4(addr), ''),
        etype = 0x86DD, dictGetOrDefault('networks_dict', 'prefix', IPv6StringToNum(addr), ''),
        ''
    )
);

CREATE FUNCTION IF NOT EXISTS getAsn AS (etype, addr) ->
(
    multiIf(
        etype = 0x0800, dictGet('geolite2_asn_dict', 'autonomous_system_number', toIPv4(addr)),
        etype = 0x86DD, dictGet('geolite2_asn_dict', 'autonomous_system_number', IPv6StringToNum(addr)),
        0
    )
);

CREATE FUNCTION IF NOT EXISTS getCountryIsoCode AS (etype, addr) ->
(
    multiIf(
        etype = 0x0800, dictGet('geolite2_country_dict', 'registered_country_iso_code', toIPv4(addr)),
        etype = 0x86DD, dictGet('geolite2_country_dict', 'registered_country_iso_code', IPv6StringToNum(addr)),
        ''
    )
);

CREATE TABLE IF NOT EXISTS flows
(
    type Int32,
    time_received_ns UInt64,
    sequence_num UInt32,
    sampling_rate UInt64,

    sampler_address String,

    time_flow_start_ns UInt64,
    time_flow_end_ns UInt64,

    bytes UInt64,
    packets UInt64,

    src_addr String,
    dst_addr String,

    etype UInt32,

    proto UInt32,

    src_port UInt32,
    dst_port UInt32,

    forwarding_status UInt32,
    tcp_flags UInt32,
    icmp_type UInt32,
    icmp_code UInt32,

    fragment_id UInt32,
    fragment_offset UInt32,

    src_asn UInt32,
    dst_asn UInt32,

    src_country String,
    dst_country String,

    src_prefix String,
    dst_prefix String
)
ENGINE = Null();

CREATE TABLE IF NOT EXISTS flows_raw
(
    date Date,

    type Int32,
    time_received DateTime64(9),
    sequence_num UInt32,
    sampling_rate UInt64,

    sampler_address String,

    time_flow_start DateTime64(9),
    time_flow_end DateTime64(9),

    bytes UInt64,
    packets UInt64,

    src_addr String,
    dst_addr String,

    etype UInt32,

    proto UInt32,

    src_port UInt32,
    dst_port UInt32,

    forwarding_status UInt32,
    tcp_flags UInt32,
    icmp_type UInt32,
    icmp_code UInt32,

    fragment_id UInt32,
    fragment_offset UInt32,

    src_asn UInt32,
    dst_asn UInt32,

    src_country String,
    dst_country String,

    src_prefix String,
    dst_prefix String
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY time_received;

CREATE MATERIALIZED VIEW IF NOT EXISTS flows_raw_mv TO flows_raw AS
    SELECT
        toDate(time_received_ns / 1000000000) AS date,
        type,
        toDateTime64(time_received_ns / 1000000000, 9) AS time_received,
        sequence_num,
        sampling_rate,
        sampler_address,
        toDateTime64(time_flow_start_ns / 1000000000, 9) AS time_flow_start,
        toDateTime64(time_flow_end_ns / 1000000000, 9) AS time_flow_end,
        bytes,
        packets,
        src_addr,
        dst_addr,
        etype,
        proto,
        src_port,
        dst_port,
        forwarding_status,
        tcp_flags,
        icmp_type,
        icmp_code,
        fragment_id,
        fragment_offset,
        getAsn(etype, src_addr) AS src_asn,
        getAsn(etype, dst_addr) AS dst_asn,
        getCountryIsoCode(etype, src_addr) AS src_country,
        getCountryIsoCode(etype, dst_addr) AS dst_country,
        getPrefix(etype, src_addr) AS src_prefix,
        getPrefix(etype, dst_addr) AS dst_prefix
    FROM flows;

CREATE VIEW IF NOT EXISTS flows_raw_pretty_view AS
    SELECT
        transform(type, [0, 1, 2, 3, 4], ['unknown', 'sflow_5', 'netflow_v5', 'netflow_v9', 'ipfix'], toString(type)) AS type,
        time_received,
        time_flow_start,
        time_flow_end,
        max2(date_diff('ms', time_flow_start, time_flow_end), 1) AS duration_ms,
        floor(bytes * sampling_rate * 8 / duration_ms) AS bps,
        floor(packets * sampling_rate / duration_ms) AS pps,
        sampler_address,
        sampling_rate,
        bytes,
        packets,
        src_addr,
        dst_addr,
        transform(etype, [0x0800, 0x0806, 0x86DD], ['ipv4', 'arp', 'ipv6'], toString(etype)) AS etype,
        transform(proto, [0x01, 0x06, 0x11, 0x3a], ['icmp', 'tcp', 'udp', 'icmp'], toString(proto)) AS proto,
        src_port,
        dst_port,
        arrayMap(x -> transform(x, [1, 2, 4, 8, 16, 32, 64, 128, 256, 512], ['fin', 'syn', 'rst', 'psh', 'ack', 'urg', 'ecn', 'cwr', 'nonce', 'reserved'], toString(x)), bitmaskToArray(tcp_flags)) as tcp_flags,
        transform(forwarding_status, [0, 1, 2, 3], ['unknown', 'forwarded', 'dropped', 'consumed'], toString(forwarding_status)) AS forwarding_status,
        fragment_offset > 0 AS is_fragment,
        src_prefix,
        dst_prefix,
        src_asn,
        dst_asn,
        src_country,
        dst_country
    FROM flows_raw
    ORDER BY time_received DESC;

CREATE VIEW IF NOT EXISTS flow_raw_by_time AS
    WITH stage1 AS (
        SELECT
            toStartOfSecond(time_received) as "time",
            sum(bytes * sampling_rate * 8) AS bps,
            sum(packets * sampling_rate) AS pps
        FROM flows_raw
        WHERE (time >= {from_time:DateTime64} AND time <= {to_time:DateTime64})
        GROUP BY time
        ORDER BY time ASC
        WITH FILL STEP INTERVAL 1 SECOND
    )
    SELECT
        time,
        floor(exponentialMovingAverage({moving_average:UInt32})(bps, toInt64(time)) OVER (Rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS bps,
        floor(exponentialMovingAverage({moving_average:UInt32})(pps, toInt64(time)) OVER (Rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS pps
    FROM stage1;

CREATE VIEW IF NOT EXISTS flow_raw_by_accurate_time_and_dst_prefix AS
    WITH stage1 AS (
        SELECT
            time_flow_end AS time,
            max2(date_diff('ms', time_flow_start, time_flow_end), 1) AS duration_ms,
            floor(bytes * sampling_rate * 8 / duration_ms) AS bps,
            floor(packets * sampling_rate / duration_ms) AS pps
        FROM
            flows_raw
        WHERE dst_prefix = {dst_prefix:String} AND time >= {from_time:DateTime64} AND time <= {to_time:DateTime64}
    )
    SELECT
        toStartOfSecond(time) AS time_flow_end_rounded,
        sum(bps / 1000000) AS mbps,
        sum(pps) AS pps
    FROM stage1
    GROUP BY time_flow_end_rounded
    ORDER BY time_flow_end_rounded ASC;
