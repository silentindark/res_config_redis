/*
 * Copyright (C) 2026 Bernardo Giordano
 *
 * This program is free software, distributed under the terms of
 * the MIT License (see LICENSE file in the top level directory of the
 * source code).
 */

/*!
 * \file
 * \brief Redis realtime configuration engine
 *
 * \author Bernardo Giordano
 *
 * This module provides a realtime configuration engine backed by Redis.
 * It stores Asterisk configuration objects (such as PJSIP endpoints, AORs,
 * auths, and contacts) as Redis Hashes, with a Redis Set maintaining an
 * index of all row IDs per table.
 *
 * \par Redis Data Model
 * \code
 *   {prefix}{table}:{id}        -> Redis HASH (columns as fields)
 *   {prefix}{table}:__index     -> Redis SET of all IDs in the table
 * \endcode
 *
 * \par Dependencies
 * - hiredis (Redis C client library)
 *
 * \par Configuration
 * See res_config_redis.conf.sample for configuration options.
 */

/*** MODULEINFO
	<support_level>extended</support_level>
 ***/

#include "asterisk.h"

#include <hiredis/hiredis.h>

#include "asterisk/config.h"
#include "asterisk/logger.h"
#include "asterisk/module.h"
#include "asterisk/lock.h"
#include "asterisk/utils.h"
#include "asterisk/cli.h"
#include "asterisk/localtime.h"

#define REDIS_CONF "res_config_redis.conf"
#define INDEX_SUFFIX ":__index"
#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 6379
#define DEFAULT_DBNUM 0
#define DEFAULT_TIMEOUT_MS 3000

static char hostname[256] = DEFAULT_HOST;
static int port = DEFAULT_PORT;
static int dbnum = DEFAULT_DBNUM;
static char password[256] = "";
static char username[256] = "";
static int timeout_ms = DEFAULT_TIMEOUT_MS;
static char prefix[256] = "";

static redisContext *redis_context;
AST_MUTEX_DEFINE_STATIC(redis_lock);
static time_t connect_time;

/*! \brief Build a Redis key from prefix, table, and id
 * \param buf Output buffer
 * \param buflen Size of output buffer
 * \param table Table name
 * \param id Row ID (may be NULL for index key)
 * \return 0 on success, -1 on truncation
 */
static int build_key(char *buf, size_t buflen, const char *table, const char *id)
{
	int res;

	if (id) {
		res = snprintf(buf, buflen, "%s%s:%s", prefix, table, id);
	} else {
		res = snprintf(buf, buflen, "%s%s" INDEX_SUFFIX, prefix, table);
	}

	return (res >= (int) buflen) ? -1 : 0;
}

/*! \brief Connect or reconnect to Redis
 * \retval 0 on success
 * \retval -1 on failure
 *
 * \note Must be called with redis_lock held
 */
static int redis_reconnect(void)
{
	struct timeval tv;
	redisReply *reply;

	/* Check existing connection */
	if (redis_context && redis_context->err == 0) {
		reply = redisCommand(redis_context, "PING");
		if (reply) {
			if (reply->type == REDIS_REPLY_STATUS &&
			    !strcasecmp(reply->str, "PONG")) {
				freeReplyObject(reply);
				return 0;
			}
			freeReplyObject(reply);
		}
	}

	/* Tear down stale connection */
	if (redis_context) {
		redisFree(redis_context);
		redis_context = NULL;
	}

	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;
	redis_context = redisConnectWithTimeout(hostname, port, tv);

	if (!redis_context || redis_context->err) {
		ast_log(LOG_ERROR, "Redis: Failed to connect to %s:%d: %s\n",
			hostname, port,
			redis_context ? redis_context->errstr : "allocation error");
		if (redis_context) {
			redisFree(redis_context);
			redis_context = NULL;
		}
		return -1;
	}

	/* Set command timeout */
	if (redisSetTimeout(redis_context, tv) != REDIS_OK) {
		ast_log(LOG_WARNING, "Redis: Failed to set command timeout\n");
	}

	/* Authenticate if configured */
	if (!ast_strlen_zero(password)) {
		if (!ast_strlen_zero(username)) {
			reply = redisCommand(redis_context, "AUTH %s %s", username, password);
		} else {
			reply = redisCommand(redis_context, "AUTH %s", password);
		}

		if (!reply || reply->type == REDIS_REPLY_ERROR) {
			ast_log(LOG_ERROR, "Redis: AUTH failed: %s\n",
				reply ? reply->str : "no reply");
			if (reply) {
				freeReplyObject(reply);
			}
			redisFree(redis_context);
			redis_context = NULL;
			return -1;
		}
		freeReplyObject(reply);
	}

	/* Select database if non-default */
	if (dbnum != 0) {
		reply = redisCommand(redis_context, "SELECT %d", dbnum);
		if (!reply || reply->type == REDIS_REPLY_ERROR) {
			ast_log(LOG_ERROR, "Redis: SELECT %d failed: %s\n",
				dbnum, reply ? reply->str : "no reply");
			if (reply) {
				freeReplyObject(reply);
			}
			redisFree(redis_context);
			redis_context = NULL;
			return -1;
		}
		freeReplyObject(reply);
	}

	connect_time = time(NULL);
	ast_log(LOG_NOTICE, "Redis: Connected to %s:%d (db %d)\n",
		hostname, port, dbnum);

	return 0;
}

/*! \brief Match a value against a SQL LIKE pattern
 * \param pattern The LIKE pattern (% = any sequence, _ = any single char)
 * \param text The text to match against
 * \retval 1 match
 * \retval 0 no match
 */
static int like_match(const char *pattern, const char *text)
{
	while (*pattern) {
		if (*pattern == '%') {
			pattern++;
			if (!*pattern) {
				return 1; /* trailing % matches everything */
			}
			while (*text) {
				if (like_match(pattern, text)) {
					return 1;
				}
				text++;
			}
			return 0;
		} else if (*pattern == '_') {
			if (!*text) {
				return 0;
			}
			pattern++;
			text++;
		} else {
			if (*pattern != *text) {
				return 0;
			}
			pattern++;
			text++;
		}
	}
	return (*text == '\0');
}

/*! \brief Check if a field/value pair matches (supports LIKE operator)
 * \param field_name The lookup field name (may contain " LIKE" suffix)
 * \param field_value The lookup value (may contain % wildcards for LIKE)
 * \param hash_value The actual value from Redis
 * \retval 1 match
 * \retval 0 no match
 */
static int field_matches(const char *field_name, const char *field_value,
	const char *hash_value)
{
	if (!hash_value) {
		return 0;
	}

	/* Check for LIKE operator in field name (e.g., "name LIKE") */
	if (strstr(field_name, " LIKE")) {
		return like_match(field_value, hash_value);
	}

	/* Exact match */
	return !strcmp(field_value, hash_value);
}

/*! \brief Get the actual field name, stripping any operator suffix
 * \param field_name The field name possibly containing " LIKE" etc.
 * \param buf Output buffer for clean field name
 * \param buflen Size of output buffer
 * \return Pointer to buf
 */
static const char *clean_field_name(const char *field_name, char *buf, size_t buflen)
{
	const char *space;

	space = strchr(field_name, ' ');
	if (space) {
		snprintf(buf, buflen, "%.*s", (int)(space - field_name), field_name);
		return buf;
	}

	return field_name;
}

/*! \brief Build an ast_variable list from a Redis HGETALL reply
 * \param reply The Redis array reply from HGETALL
 * \return ast_variable linked list, or NULL on error/empty
 */
static struct ast_variable *reply_to_variables(redisReply *reply)
{
	struct ast_variable *head = NULL;
	struct ast_variable *tail = NULL;
	struct ast_variable *var;
	size_t i;

	if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
		return NULL;
	}

	/* HGETALL returns alternating field, value pairs */
	for (i = 0; i + 1 < reply->elements; i += 2) {
		if (reply->element[i]->type != REDIS_REPLY_STRING ||
		    reply->element[i + 1]->type != REDIS_REPLY_STRING) {
			continue;
		}

		/* Skip fields with empty string values — returning them would
		 * cause Asterisk to treat them as explicitly set (e.g. an empty
		 * 'contact' on an AOR prevents dynamic contact creation). */
		if (reply->element[i + 1]->len == 0) {
			continue;
		}

		var = ast_variable_new(reply->element[i]->str,
			reply->element[i + 1]->str, "");
		if (!var) {
			ast_variables_destroy(head);
			return NULL;
		}

		if (tail) {
			tail->next = var;
		} else {
			head = var;
		}
		tail = var;
	}

	return head;
}

/*! \brief Check if an ast_variable list matches all filter conditions
 * \param vars The variable list (from a Redis hash)
 * \param fields The filter conditions
 * \retval 1 all conditions match
 * \retval 0 at least one condition doesn't match
 */
static int variables_match_fields(struct ast_variable *vars,
	const struct ast_variable *fields)
{
	const struct ast_variable *filter;
	struct ast_variable *v;
	char clean_name[256];
	const char *name;
	int found;

	for (filter = fields; filter; filter = filter->next) {
		name = clean_field_name(filter->name, clean_name, sizeof(clean_name));
		found = 0;

		for (v = vars; v; v = v->next) {
			if (!strcasecmp(v->name, name)) {
				if (field_matches(filter->name, filter->value, v->value)) {
					found = 1;
				}
				break;
			}
		}

		if (!found) {
			return 0;
		}
	}

	return 1;
}

/*! \brief Find the value of a named field in a variable list
 * \param vars The variable list
 * \param name The field name to find
 * \return The field value, or NULL if not found
 */
static const char *find_variable_value(const struct ast_variable *vars,
	const char *name)
{
	const struct ast_variable *v;

	for (v = vars; v; v = v->next) {
		if (!strcasecmp(v->name, name)) {
			return v->value;
		}
	}

	return NULL;
}

/*
 * Realtime engine callbacks
 */

/*! \brief Load a single row from Redis by lookup fields
 *
 * This is the primary single-object lookup used by sorcery.
 * When looking up by 'id', does a direct HGETALL.
 * For other fields, scans the index and filters.
 */
static struct ast_variable *realtime_redis(const char *database,
	const char *table, const struct ast_variable *fields)
{
	char key[1024];
	redisReply *reply = NULL;
	redisReply *index_reply = NULL;
	struct ast_variable *result = NULL;
	const struct ast_variable *first_field;

	if (!fields) {
		return NULL;
	}

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_mutex_unlock(&redis_lock);
		return NULL;
	}

	first_field = fields;

	/* Optimized path: single lookup by 'id' */
	if (!strcasecmp(first_field->name, "id") && !first_field->next) {
		if (build_key(key, sizeof(key), table, first_field->value)) {
			ast_mutex_unlock(&redis_lock);
			return NULL;
		}

		reply = redisCommand(redis_context, "HGETALL %s", key);
		if (!reply) {
			ast_log(LOG_ERROR, "Redis: HGETALL %s failed: %s\n",
				key, redis_context->errstr);
			ast_mutex_unlock(&redis_lock);
			return NULL;
		}

		if (reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
			result = reply_to_variables(reply);
		}

		freeReplyObject(reply);
		ast_mutex_unlock(&redis_lock);

		/* Return empty result sentinel if no data found */
		if (!result) {
			return NULL;
		}
		return result;
	}

	/* General path: scan index and filter */
	if (build_key(key, sizeof(key), table, NULL)) {
		ast_mutex_unlock(&redis_lock);
		return NULL;
	}

	index_reply = redisCommand(redis_context, "SMEMBERS %s", key);
	if (!index_reply || index_reply->type != REDIS_REPLY_ARRAY) {
		if (index_reply) {
			freeReplyObject(index_reply);
		}
		ast_mutex_unlock(&redis_lock);
		return NULL;
	}

	for (size_t i = 0; i < index_reply->elements; i++) {
		struct ast_variable *row_vars;

		if (index_reply->element[i]->type != REDIS_REPLY_STRING) {
			continue;
		}

		if (build_key(key, sizeof(key), table, index_reply->element[i]->str)) {
			continue;
		}

		reply = redisCommand(redis_context, "HGETALL %s", key);
		if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
			if (reply) {
				freeReplyObject(reply);
			}
			continue;
		}

		row_vars = reply_to_variables(reply);
		freeReplyObject(reply);

		if (!row_vars) {
			continue;
		}

		if (variables_match_fields(row_vars, fields)) {
			result = row_vars;
			break;
		}

		ast_variables_destroy(row_vars);
	}

	freeReplyObject(index_reply);
	ast_mutex_unlock(&redis_lock);

	return result;
}

/*! \brief Load multiple rows from Redis matching the given fields */
static struct ast_config *realtime_multi_redis(const char *database,
	const char *table, const struct ast_variable *fields)
{
	char key[1024];
	struct ast_config *cfg = NULL;
	redisReply *index_reply = NULL;
	redisReply *reply = NULL;

	if (!fields) {
		return NULL;
	}

	cfg = ast_config_new();
	if (!cfg) {
		return NULL;
	}

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_mutex_unlock(&redis_lock);
		ast_config_destroy(cfg);
		return NULL;
	}

	/* Get all IDs in the table */
	if (build_key(key, sizeof(key), table, NULL)) {
		ast_mutex_unlock(&redis_lock);
		ast_config_destroy(cfg);
		return NULL;
	}

	index_reply = redisCommand(redis_context, "SMEMBERS %s", key);
	if (!index_reply || index_reply->type != REDIS_REPLY_ARRAY) {
		if (index_reply) {
			freeReplyObject(index_reply);
		}
		ast_mutex_unlock(&redis_lock);
		return cfg; /* Return empty config, not NULL (not an error) */
	}

	for (size_t i = 0; i < index_reply->elements; i++) {
		struct ast_variable *row_vars;
		struct ast_variable *v;
		struct ast_category *cat;

		if (index_reply->element[i]->type != REDIS_REPLY_STRING) {
			continue;
		}

		if (build_key(key, sizeof(key), table, index_reply->element[i]->str)) {
			continue;
		}

		reply = redisCommand(redis_context, "HGETALL %s", key);
		if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
			if (reply) {
				freeReplyObject(reply);
			}
			continue;
		}

		row_vars = reply_to_variables(reply);
		freeReplyObject(reply);

		if (!row_vars) {
			continue;
		}

		if (!variables_match_fields(row_vars, fields)) {
			ast_variables_destroy(row_vars);
			continue;
		}

		/* Create an anonymous category for this row */
		cat = ast_category_new_anonymous();
		if (!cat) {
			ast_variables_destroy(row_vars);
			continue;
		}

		/* Transfer variables into the category.
		 * ast_variable_append takes ownership, so we detach from row_vars
		 * one by one to avoid double-free. */
		while (row_vars) {
			v = row_vars;
			row_vars = row_vars->next;
			v->next = NULL;
			ast_variable_append(cat, v);
		}

		ast_category_append(cfg, cat);
	}

	freeReplyObject(index_reply);
	ast_mutex_unlock(&redis_lock);

	return cfg;
}

/*! \brief Store (INSERT) a new row into Redis */
static int store_redis(const char *database, const char *table,
	const struct ast_variable *fields)
{
	char key[1024];
	char index_key[1024];
	const char *id;
	const struct ast_variable *v;
	redisReply *reply;
	int count;

	if (!fields) {
		return -1;
	}

	id = find_variable_value(fields, "id");
	if (ast_strlen_zero(id)) {
		ast_log(LOG_ERROR, "Redis: store to '%s' requires an 'id' field\n", table);
		return -1;
	}

	if (build_key(key, sizeof(key), table, id) ||
	    build_key(index_key, sizeof(index_key), table, NULL)) {
		return -1;
	}

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_mutex_unlock(&redis_lock);
		return -1;
	}

	/* Delete any existing key first to ensure clean state */
	reply = redisCommand(redis_context, "DEL %s", key);
	if (reply) {
		freeReplyObject(reply);
	}

	/* Store each field using individual HSET commands */
	count = 0;
	for (v = fields; v; v = v->next) {
		reply = redisCommand(redis_context, "HSET %s %s %s",
			key, v->name, v->value);
		if (!reply) {
			ast_log(LOG_ERROR, "Redis: HSET %s %s failed: %s\n",
				key, v->name, redis_context->errstr);
			ast_mutex_unlock(&redis_lock);
			return -1;
		}
		if (reply->type == REDIS_REPLY_ERROR) {
			ast_log(LOG_ERROR, "Redis: HSET %s %s error: %s\n",
				key, v->name, reply->str);
			freeReplyObject(reply);
			ast_mutex_unlock(&redis_lock);
			return -1;
		}
		freeReplyObject(reply);
		count++;
	}

	/* Add to the index set */
	reply = redisCommand(redis_context, "SADD %s %s", index_key, id);
	if (!reply || reply->type == REDIS_REPLY_ERROR) {
		ast_log(LOG_WARNING, "Redis: SADD %s %s failed: %s\n",
			index_key, id,
			reply ? reply->str : redis_context->errstr);
		if (reply) {
			freeReplyObject(reply);
		}
		/* Don't fail the whole store for an index error */
	} else {
		freeReplyObject(reply);
	}

	ast_mutex_unlock(&redis_lock);

	ast_debug(3, "Redis: Stored %d fields in %s\n", count, key);
	return 1;
}

/*! \brief Delete rows from Redis */
static int destroy_redis(const char *database, const char *table,
	const char *keyfield, const char *entity,
	const struct ast_variable *fields)
{
	char key[1024];
	char index_key[1024];
	redisReply *reply;
	int deleted = 0;

	if (build_key(index_key, sizeof(index_key), table, NULL)) {
		return -1;
	}

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_mutex_unlock(&redis_lock);
		return -1;
	}

	/* Direct delete if keyfield is 'id' and no extra conditions */
	if (!strcasecmp(keyfield, "id") && !fields) {
		if (build_key(key, sizeof(key), table, entity)) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}

		reply = redisCommand(redis_context, "DEL %s", key);
		if (!reply) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}
		if (reply->type == REDIS_REPLY_INTEGER) {
			deleted = (int) reply->integer;
		}
		freeReplyObject(reply);

		/* Remove from index */
		reply = redisCommand(redis_context, "SREM %s %s", index_key, entity);
		if (reply) {
			freeReplyObject(reply);
		}

		ast_mutex_unlock(&redis_lock);
		return deleted;
	}

	/* General case: scan index and match */
	{
		redisReply *index_reply;
		size_t i;

		index_reply = redisCommand(redis_context, "SMEMBERS %s", index_key);
		if (!index_reply || index_reply->type != REDIS_REPLY_ARRAY) {
			if (index_reply) {
				freeReplyObject(index_reply);
			}
			ast_mutex_unlock(&redis_lock);
			return -1;
		}

		for (i = 0; i < index_reply->elements; i++) {
			struct ast_variable *row_vars;
			const char *row_id;
			int match;

			if (index_reply->element[i]->type != REDIS_REPLY_STRING) {
				continue;
			}

			row_id = index_reply->element[i]->str;

			if (build_key(key, sizeof(key), table, row_id)) {
				continue;
			}

			reply = redisCommand(redis_context, "HGETALL %s", key);
			if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
				if (reply) {
					freeReplyObject(reply);
				}
				continue;
			}

			row_vars = reply_to_variables(reply);
			freeReplyObject(reply);

			if (!row_vars) {
				continue;
			}

			/* Check primary keyfield match */
			match = 0;
			{
				const char *val = find_variable_value(row_vars, keyfield);
				if (val && !strcmp(val, entity)) {
					match = 1;
				}
			}

			/* Check additional field conditions */
			if (match && fields) {
				match = variables_match_fields(row_vars, fields);
			}

			ast_variables_destroy(row_vars);

			if (match) {
				reply = redisCommand(redis_context, "DEL %s", key);
				if (reply) {
					if (reply->type == REDIS_REPLY_INTEGER && reply->integer > 0) {
						deleted++;
					}
					freeReplyObject(reply);
				}

				reply = redisCommand(redis_context, "SREM %s %s", index_key, row_id);
				if (reply) {
					freeReplyObject(reply);
				}
			}
		}

		freeReplyObject(index_reply);
	}

	ast_mutex_unlock(&redis_lock);

	return deleted;
}

/*! \brief Update rows in Redis (single lookup field) */
static int update_redis(const char *database, const char *table,
	const char *keyfield, const char *entity,
	const struct ast_variable *fields)
{
	char key[1024];
	redisReply *reply;
	const struct ast_variable *v;
	int updated = 0;

	if (!fields) {
		return -1;
	}

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_mutex_unlock(&redis_lock);
		return -1;
	}

	/* Direct update if keyfield is 'id' */
	if (!strcasecmp(keyfield, "id")) {
		if (build_key(key, sizeof(key), table, entity)) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}

		/* Verify the key exists */
		reply = redisCommand(redis_context, "EXISTS %s", key);
		if (!reply) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}
		if (reply->type != REDIS_REPLY_INTEGER || reply->integer == 0) {
			freeReplyObject(reply);
			ast_mutex_unlock(&redis_lock);
			return 0;
		}
		freeReplyObject(reply);

		for (v = fields; v; v = v->next) {
			reply = redisCommand(redis_context, "HSET %s %s %s",
				key, v->name, v->value);
			if (!reply) {
				ast_mutex_unlock(&redis_lock);
				return -1;
			}
			if (reply->type == REDIS_REPLY_ERROR) {
				ast_log(LOG_ERROR, "Redis: HSET %s %s error: %s\n",
					key, v->name, reply->str);
				freeReplyObject(reply);
				ast_mutex_unlock(&redis_lock);
				return -1;
			}
			freeReplyObject(reply);
		}

		ast_mutex_unlock(&redis_lock);
		return 1;
	}

	/* General case: scan index and match */
	{
		char index_key[1024];
		redisReply *index_reply;
		size_t i;

		if (build_key(index_key, sizeof(index_key), table, NULL)) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}

		index_reply = redisCommand(redis_context, "SMEMBERS %s", index_key);
		if (!index_reply || index_reply->type != REDIS_REPLY_ARRAY) {
			if (index_reply) {
				freeReplyObject(index_reply);
			}
			ast_mutex_unlock(&redis_lock);
			return -1;
		}

		for (i = 0; i < index_reply->elements; i++) {
			struct ast_variable *row_vars;
			const char *val;

			if (index_reply->element[i]->type != REDIS_REPLY_STRING) {
				continue;
			}

			if (build_key(key, sizeof(key), table,
				       index_reply->element[i]->str)) {
				continue;
			}

			reply = redisCommand(redis_context, "HGETALL %s", key);
			if (!reply || reply->type != REDIS_REPLY_ARRAY ||
			    reply->elements == 0) {
				if (reply) {
					freeReplyObject(reply);
				}
				continue;
			}

			row_vars = reply_to_variables(reply);
			freeReplyObject(reply);

			if (!row_vars) {
				continue;
			}

			val = find_variable_value(row_vars, keyfield);
			if (val && !strcmp(val, entity)) {
				/* Match found, apply updates */
				for (v = fields; v; v = v->next) {
					reply = redisCommand(redis_context,
						"HSET %s %s %s",
						key, v->name, v->value);
					if (reply) {
						freeReplyObject(reply);
					}
				}
				updated++;
			}

			ast_variables_destroy(row_vars);
		}

		freeReplyObject(index_reply);
	}

	ast_mutex_unlock(&redis_lock);

	return updated;
}

/*! \brief Update rows in Redis (multiple lookup fields) */
static int update2_redis(const char *database, const char *table,
	const struct ast_variable *lookup_fields,
	const struct ast_variable *update_fields)
{
	char key[1024];
	char index_key[1024];
	redisReply *index_reply = NULL;
	redisReply *reply = NULL;
	const struct ast_variable *v;
	int updated = 0;

	if (!lookup_fields || !update_fields) {
		return -1;
	}

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_mutex_unlock(&redis_lock);
		return -1;
	}

	/* Check for direct id lookup optimization */
	if (!strcasecmp(lookup_fields->name, "id") && !lookup_fields->next) {
		if (build_key(key, sizeof(key), table, lookup_fields->value)) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}

		/* Verify the key exists */
		reply = redisCommand(redis_context, "EXISTS %s", key);
		if (!reply) {
			ast_mutex_unlock(&redis_lock);
			return -1;
		}
		if (reply->type != REDIS_REPLY_INTEGER || reply->integer == 0) {
			freeReplyObject(reply);
			ast_mutex_unlock(&redis_lock);
			return 0;
		}
		freeReplyObject(reply);

		for (v = update_fields; v; v = v->next) {
			reply = redisCommand(redis_context, "HSET %s %s %s",
				key, v->name, v->value);
			if (reply) {
				freeReplyObject(reply);
			}
		}

		ast_mutex_unlock(&redis_lock);
		return 1;
	}

	/* General case: scan and filter */
	if (build_key(index_key, sizeof(index_key), table, NULL)) {
		ast_mutex_unlock(&redis_lock);
		return -1;
	}

	index_reply = redisCommand(redis_context, "SMEMBERS %s", index_key);
	if (!index_reply || index_reply->type != REDIS_REPLY_ARRAY) {
		if (index_reply) {
			freeReplyObject(index_reply);
		}
		ast_mutex_unlock(&redis_lock);
		return -1;
	}

	for (size_t i = 0; i < index_reply->elements; i++) {
		struct ast_variable *row_vars;

		if (index_reply->element[i]->type != REDIS_REPLY_STRING) {
			continue;
		}

		if (build_key(key, sizeof(key), table,
			       index_reply->element[i]->str)) {
			continue;
		}

		reply = redisCommand(redis_context, "HGETALL %s", key);
		if (!reply || reply->type != REDIS_REPLY_ARRAY ||
		    reply->elements == 0) {
			if (reply) {
				freeReplyObject(reply);
			}
			continue;
		}

		row_vars = reply_to_variables(reply);
		freeReplyObject(reply);

		if (!row_vars) {
			continue;
		}

		if (variables_match_fields(row_vars, lookup_fields)) {
			for (v = update_fields; v; v = v->next) {
				reply = redisCommand(redis_context,
					"HSET %s %s %s",
					key, v->name, v->value);
				if (reply) {
					freeReplyObject(reply);
				}
			}
			updated++;
		}

		ast_variables_destroy(row_vars);
	}

	freeReplyObject(index_reply);
	ast_mutex_unlock(&redis_lock);

	return updated;
}

/*! \brief Schema validation (no-op for Redis - it's schemaless) */
static int require_redis(const char *database, const char *table, va_list ap)
{
	/* Redis is schemaless - always succeed */
	return 0;
}

/*! \brief Cache unload (no-op for Redis - no local schema cache) */
static int unload_redis(const char *database, const char *table)
{
	return 0;
}

/*! \brief Static configuration loading (stub - not implemented) */
static struct ast_config *config_redis(const char *database, const char *table,
	const char *file, struct ast_config *cfg, struct ast_flags flags,
	const char *suggested_include_file, const char *who_asked)
{
	return NULL;
}

static struct ast_config_engine redis_engine = {
	.name = "redis",
	.load_func = config_redis,
	.realtime_func = realtime_redis,
	.realtime_multi_func = realtime_multi_redis,
	.store_func = store_redis,
	.destroy_func = destroy_redis,
	.update_func = update_redis,
	.update2_func = update2_redis,
	.require_func = require_redis,
	.unload_func = unload_redis,
};

/*
 * CLI commands
 */

static char *handle_cli_redis_show_status(struct ast_cli_entry *e,
	int cmd, struct ast_cli_args *a)
{
	switch (cmd) {
	case CLI_INIT:
		e->command = "redis show status";
		e->usage =
			"Usage: redis show status\n"
			"       Shows the current Redis connection status.\n";
		return NULL;
	case CLI_GENERATE:
		return NULL;
	}

	if (a->argc != 3) {
		return CLI_SHOWUSAGE;
	}

	ast_mutex_lock(&redis_lock);

	if (!redis_context) {
		ast_cli(a->fd, "Redis: Not connected\n");
		ast_cli(a->fd, "  Configured: %s:%d (db %d)\n", hostname, port, dbnum);
	} else if (redis_context->err) {
		ast_cli(a->fd, "Redis: Connection error: %s\n", redis_context->errstr);
		ast_cli(a->fd, "  Configured: %s:%d (db %d)\n", hostname, port, dbnum);
	} else {
		redisReply *reply;
		char timebuf[64];
		struct ast_tm tm;
		struct timeval tv;
		time_t now;

		ast_cli(a->fd, "Redis: Connected to %s:%d (db %d)\n",
			hostname, port, dbnum);

		if (connect_time) {
			tv.tv_sec = connect_time;
			tv.tv_usec = 0;
			ast_localtime(&tv, &tm, NULL);
			ast_strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", &tm);
			now = time(NULL);
			ast_cli(a->fd, "  Connected since: %s (%" PRId64 " seconds)\n",
				timebuf, (int64_t)(now - connect_time));
		}

		if (!ast_strlen_zero(prefix)) {
			ast_cli(a->fd, "  Key prefix: %s\n", prefix);
		}

		if (!ast_strlen_zero(username)) {
			ast_cli(a->fd, "  Username: %s\n", username);
		}

		reply = redisCommand(redis_context, "INFO server");
		if (reply && reply->type == REDIS_REPLY_STRING) {
			/* Extract redis_version from INFO output */
			const char *ver = strstr(reply->str, "redis_version:");
			if (ver) {
				char version[64] = "";
				sscanf(ver, "redis_version:%63[^\r\n]", version);
				ast_cli(a->fd, "  Server version: %s\n", version);
			}
		}
		if (reply) {
			freeReplyObject(reply);
		}
	}

	ast_mutex_unlock(&redis_lock);

	return CLI_SUCCESS;
}

static char *handle_cli_redis_show_records(struct ast_cli_entry *e,
	int cmd, struct ast_cli_args *a)
{
	char index_key[1024];
	char key[1024];
	redisReply *index_reply;
	redisReply *reply;
	const char *table;
	int count = 0;

	switch (cmd) {
	case CLI_INIT:
		e->command = "redis show records";
		e->usage =
			"Usage: redis show records <table>\n"
			"       Shows all records stored in Redis for the given table.\n";
		return NULL;
	case CLI_GENERATE:
		return NULL;
	}

	if (a->argc != 4) {
		return CLI_SHOWUSAGE;
	}

	table = a->argv[3];

	ast_mutex_lock(&redis_lock);

	if (redis_reconnect()) {
		ast_cli(a->fd, "Redis: Not connected\n");
		ast_mutex_unlock(&redis_lock);
		return CLI_FAILURE;
	}

	if (build_key(index_key, sizeof(index_key), table, NULL)) {
		ast_mutex_unlock(&redis_lock);
		return CLI_FAILURE;
	}

	index_reply = redisCommand(redis_context, "SMEMBERS %s", index_key);
	if (!index_reply || index_reply->type != REDIS_REPLY_ARRAY) {
		ast_cli(a->fd, "Redis: No records found for table '%s'\n", table);
		if (index_reply) {
			freeReplyObject(index_reply);
		}
		ast_mutex_unlock(&redis_lock);
		return CLI_SUCCESS;
	}

	ast_cli(a->fd, "Table: %s (%zu records in index)\n\n",
		table, index_reply->elements);

	for (size_t i = 0; i < index_reply->elements; i++) {
		if (index_reply->element[i]->type != REDIS_REPLY_STRING) {
			continue;
		}

		if (build_key(key, sizeof(key), table,
			       index_reply->element[i]->str)) {
			continue;
		}

		reply = redisCommand(redis_context, "HGETALL %s", key);
		if (!reply || reply->type != REDIS_REPLY_ARRAY ||
		    reply->elements == 0) {
			if (reply) {
				freeReplyObject(reply);
			}
			continue;
		}

		ast_cli(a->fd, "[%s]\n", index_reply->element[i]->str);

		for (size_t j = 0; j + 1 < reply->elements; j += 2) {
			if (reply->element[j]->type == REDIS_REPLY_STRING &&
			    reply->element[j + 1]->type == REDIS_REPLY_STRING) {
				ast_cli(a->fd, "  %-30s = %s\n",
					reply->element[j]->str,
					reply->element[j + 1]->str);
			}
		}

		ast_cli(a->fd, "\n");
		freeReplyObject(reply);
		count++;
	}

	ast_cli(a->fd, "Total: %d records\n", count);

	freeReplyObject(index_reply);
	ast_mutex_unlock(&redis_lock);

	return CLI_SUCCESS;
}

static struct ast_cli_entry cli_realtime_redis[] = {
	AST_CLI_DEFINE(handle_cli_redis_show_status, "Show Redis connection status"),
	AST_CLI_DEFINE(handle_cli_redis_show_records, "Show all records for a Redis table"),
};

/*
 * Configuration loading
 */

static int parse_config(int reload)
{
	struct ast_config *cfg;
	struct ast_flags config_flags = { reload ? CONFIG_FLAG_FILEUNCHANGED : 0 };
	const char *val;

	cfg = ast_config_load(REDIS_CONF, config_flags);

	if (cfg == CONFIG_STATUS_FILEUNCHANGED) {
		return 0;
	}

	if (cfg == CONFIG_STATUS_FILEINVALID) {
		ast_log(LOG_ERROR, "Redis: Config file %s is invalid\n", REDIS_CONF);
		return -1;
	}

	if (!cfg) {
		ast_log(LOG_WARNING, "Redis: Unable to load config %s, using defaults\n",
			REDIS_CONF);
		return 0;
	}

	ast_mutex_lock(&redis_lock);

	val = ast_variable_retrieve(cfg, "general", "hostname");
	if (!ast_strlen_zero(val)) {
		ast_copy_string(hostname, val, sizeof(hostname));
	} else {
		ast_copy_string(hostname, DEFAULT_HOST, sizeof(hostname));
	}

	val = ast_variable_retrieve(cfg, "general", "port");
	if (!ast_strlen_zero(val)) {
		port = atoi(val);
		if (port <= 0 || port > 65535) {
			port = DEFAULT_PORT;
		}
	} else {
		port = DEFAULT_PORT;
	}

	val = ast_variable_retrieve(cfg, "general", "dbnum");
	if (!ast_strlen_zero(val)) {
		dbnum = atoi(val);
		if (dbnum < 0) {
			dbnum = DEFAULT_DBNUM;
		}
	} else {
		dbnum = DEFAULT_DBNUM;
	}

	val = ast_variable_retrieve(cfg, "general", "password");
	if (!ast_strlen_zero(val)) {
		ast_copy_string(password, val, sizeof(password));
	} else {
		password[0] = '\0';
	}

	val = ast_variable_retrieve(cfg, "general", "username");
	if (!ast_strlen_zero(val)) {
		ast_copy_string(username, val, sizeof(username));
	} else {
		username[0] = '\0';
	}

	val = ast_variable_retrieve(cfg, "general", "timeout");
	if (!ast_strlen_zero(val)) {
		timeout_ms = atoi(val);
		if (timeout_ms < 100) {
			timeout_ms = DEFAULT_TIMEOUT_MS;
		}
	} else {
		timeout_ms = DEFAULT_TIMEOUT_MS;
	}

	val = ast_variable_retrieve(cfg, "general", "prefix");
	if (!ast_strlen_zero(val)) {
		ast_copy_string(prefix, val, sizeof(prefix));
	} else {
		prefix[0] = '\0';
	}

	/* Force reconnect on config change */
	if (redis_context) {
		redisFree(redis_context);
		redis_context = NULL;
	}

	ast_mutex_unlock(&redis_lock);

	ast_config_destroy(cfg);

	return 0;
}

/*
 * Module lifecycle
 */

static int load_module(void)
{
	if (parse_config(0)) {
		return AST_MODULE_LOAD_DECLINE;
	}

	ast_config_engine_register(&redis_engine);
	ast_cli_register_multiple(cli_realtime_redis, ARRAY_LEN(cli_realtime_redis));

	ast_log(LOG_NOTICE, "Redis RealTime driver loaded\n");

	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{
	ast_cli_unregister_multiple(cli_realtime_redis, ARRAY_LEN(cli_realtime_redis));
	ast_config_engine_deregister(&redis_engine);

	ast_mutex_lock(&redis_lock);
	if (redis_context) {
		redisFree(redis_context);
		redis_context = NULL;
	}
	ast_mutex_unlock(&redis_lock);

	ast_log(LOG_NOTICE, "Redis RealTime driver unloaded\n");

	return 0;
}

static int reload(void)
{
	parse_config(1);
	return 0;
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_LOAD_ORDER, "Redis RealTime Configuration Driver",
	.support_level = AST_MODULE_SUPPORT_EXTENDED,
	.load = load_module,
	.unload = unload_module,
	.reload = reload,
	.load_pri = AST_MODPRI_REALTIME_DRIVER,
	.requires = "extconfig",
);
