/**
 * Neon HTTP non-interactive transactions (ARCH-025 · N12).
 *
 * Product client stays on `drizzle-orm/neon-http` over `-pooler`. Interactive
 * `db.transaction` is unsupported on neon-http; use `sql.transaction([...])`
 * for atomic multi-statement writes in one HTTP round-trip.
 */

import type {
	HTTPTransactionOptions,
	NeonQueryFunction,
	NeonQueryPromise,
} from "@neondatabase/serverless";
import { neon } from "@neondatabase/serverless";

import { requireProductDatabaseUrl } from "./env";

export type NeonHttpIsolationLevel = NonNullable<
	HTTPTransactionOptions<false, false>["isolationLevel"]
>;

export type NeonHttpTransactionOptions = {
	isolationLevel?: NeonHttpIsolationLevel;
	readOnly?: boolean;
	deferrable?: boolean;
};

export type NeonHttpSql = NeonQueryFunction<false, false>;

let cachedSql: NeonHttpSql | undefined;

/**
 * Shared Neon HTTP SQL client (same instance as the Drizzle product client).
 * Lazy: no connection until first call.
 */
export function getNeonSql(): NeonHttpSql {
	cachedSql ??= neon(requireProductDatabaseUrl());
	return cachedSql;
}

type NeonHttpTxQuery = NeonQueryPromise<false, false>;

/**
 * Run queries in one Neon HTTP non-interactive transaction.
 * Default isolation: `ReadCommitted` (Postgres write default; explicit for ops).
 */
export async function runNeonHttpTransaction<T extends unknown[]>(
	queriesOrFn: NeonHttpTxQuery[] | ((sql: NeonHttpSql) => NeonHttpTxQuery[]),
	options?: NeonHttpTransactionOptions,
): Promise<T> {
	// Resolve the query list without eagerly connecting to Neon.
	// A lazy Proxy is used so that when the builder function never accesses
	// `sql` (e.g. it returns []), the empty-list guard fires before
	// requireProductDatabaseUrl() is ever called — matching the contract test.
	//
	// The Proxy target must be a no-op function because NeonQueryFunction is
	// itself callable (tagged-template); a plain {} target would cause a
	// "target is not a function" TypeError on the apply trap.
	const queries: NeonHttpTxQuery[] =
		typeof queriesOrFn === "function"
			? queriesOrFn(
					new Proxy((() => {}) as unknown as NeonHttpSql, {
						get(_, prop) {
							return Reflect.get(getNeonSql() as object, prop);
						},
						apply(_, thisArg, argList) {
							return Reflect.apply(
								getNeonSql() as unknown as (...args: unknown[]) => unknown,
								thisArg as NeonHttpSql,
								argList,
							);
						},
					}),
				)
			: queriesOrFn;

	if (queries.length === 0) {
		throw new Error("runNeonHttpTransaction requires at least one query");
	}

	const sql = getNeonSql();
	const result = await sql.transaction(queries, {
		isolationLevel: options?.isolationLevel ?? "ReadCommitted",
		readOnly: options?.readOnly,
		deferrable: options?.deferrable,
	});

	return result as T;
}
