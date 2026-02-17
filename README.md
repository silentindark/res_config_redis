# res_config_redis

Asterisk realtime configuration engine backed by Redis. Stores PJSIP sorcery objects (endpoints, AORs, auths, contacts) as Redis Hashes.

## Requirements

- Asterisk (tested on >= 22.x source tree)
- hiredis development library
- Redis server

## Install hiredis

You can find `hiredis` on your favourite OS. For instance:

```bash
apt install libhiredis-dev
```

## Build

1. Copy the module source into your Asterisk source tree:

```bash
cp res_config_redis.c /path/to/asterisk-22.x/res/
```

2. Tell the Asterisk build system to link hiredis. Add this line to `res/Makefile`:

```makefile
res_config_redis.so: LIBS+=-lhiredis
```

Find the section in `res/Makefile` where other modules have similar `LIBS+=` lines (e.g., `res_config_pgsql.so`) and add it there.

3. Build the module. Make sure `./configure` has already been run in the Asterisk source tree.

Build everything (simplest):

```bash
cd /path/to/asterisk-22.x
make
```

## Configure

### 1. Redis module config

Copy the sample config to your Asterisk config directory:

```bash
cp res_config_redis.conf.sample /etc/asterisk/res_config_redis.conf
```

Edit `/etc/asterisk/res_config_redis.conf`:

```ini
[general]
hostname = 127.0.0.1
port = 6379
dbnum = 0
;password = your_redis_password
;username = your_redis_username
timeout = 3000
;prefix = ast:
```

### 2. Map PJSIP tables to Redis

Edit `/etc/asterisk/extconfig.conf`:

```ini
[settings]
ps_endpoints => redis,general
ps_aors => redis,general
ps_auths => redis,general
ps_contacts => redis,general
```

### 3. Configure PJSIP to use realtime

Edit `/etc/asterisk/sorcery.conf`:

```ini
[res_pjsip]
endpoint=realtime,ps_endpoints
auth=realtime,ps_auths
aor=realtime,ps_aors
contact=realtime,ps_contacts

[res_pjsip_endpoint_identifier_ip]
identify=realtime,ps_endpoint_id_ips
```

## Populate Redis

Use `redis-cli` to add PJSIP objects:

```bash
# Create an endpoint
redis-cli HSET ps_endpoints:1001 id 1001 transport transport-udp aors 1001 auth auth1001 context from-internal disallow all allow "ulaw,alaw"
redis-cli SADD ps_endpoints:__index 1001

# Create an AOR
redis-cli HSET ps_aors:1001 id 1001 max_contacts 1
redis-cli SADD ps_aors:__index 1001

# Create auth credentials
redis-cli HSET ps_auths:1001 id 1001 auth_type userpass username 1001 password mysecret
redis-cli SADD ps_auths:__index 1001
```

Every row must have an `id` field, and the ID must be added to the table's `__index` set.

## Verify

Start or restart Asterisk and check:

```bash
# Check module is loaded
asterisk -rx "module show like redis"

# Check connection status
asterisk -rx "redis show status"

# List all records in a table
asterisk -rx "redis show records ps_endpoints"

# Verify PJSIP sees the endpoint
asterisk -rx "pjsip show endpoints"
asterisk -rx "pjsip show endpoint 1001"
```

## Redis Data Model

| Redis Key | Type | Description |
|-----------|------|-------------|
| `{table}:{id}` | HASH | One hash per row, fields are columns |
| `{table}:__index` | SET | Set of all row IDs in the table |

When using a prefix (e.g., `prefix = ast:`), all keys are prefixed: `ast:ps_endpoints:1001`.

## CLI Commands

| Command | Description |
|---------|-------------|
| `redis show status` | Show connection state, server version, uptime |
| `redis show records <table>` | Dump all records for a table |

## License

This project is licensed under the MIT license. See [LICENSE](https://github.com/BernardoGiordano/res_config_redis/blob/main/LICENSE) for details.