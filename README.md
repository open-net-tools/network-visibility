# NetSight

NetSight is a network monitoring tool that allows you to monitor network traffic in real time using NetFlow, IPFix, and sFlow.

## Architecture

-

## Installation

### GeoLite2

You can download the GeoLite2 database from MaxMind or from [this](https://github.com/P3TERX/GeoLite.mmdb) repository. (Another option is to use [ip-location-db](https://github.com/sapics/ip-location-db)).

```bash
wget https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-ASN.mmdb
wget https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb
```

Download [mmdbctl](https://github.com/ipinfo/mmdbctl) and convert the database to `csv` format:

```bash
# NOTE: change the platform to your platform
wget https://github.com/ipinfo/mmdbctl/releases/download/mmdbctl-1.4.6/mmdbctl_1.4.6_darwin_arm64.tar.gz
tar -xvf mmdbctl_1.4.6_darwin_arm64.tar.gz mmdbctl
./mmdbctl_1.4.6_darwin_arm64 export GeoLite2-ASN.mmdb ./clickhouse/data/GeoLite2-ASN.csv

# We convert the GeoLite2-Country.mmdb to JSON, extract the relevant fields, and then convert it to CSV
./mmdbctl_1.4.6_darwin_arm64 export -f json GeoLite2-Country.mmdb GeoLite2-Country.json
printf "range,registered_country_iso_code,registered_country_name_en\n" > ./clickhouse/data/GeoLite2-Country.csv
jq -r '[ .range, .registered_country.iso_code, .registered_country.names.en ] | @csv' GeoLite2-Country.json >> ./clickhouse/data/GeoLite2-Country.csv
```
