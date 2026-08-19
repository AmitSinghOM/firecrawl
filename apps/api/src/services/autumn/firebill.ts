import { config } from "../../config";
import { logger } from "../../lib/logger";
import { sampled } from "../../lib/rollout";
import type { TrackParams } from "./types";
import {
  billingRouteTotal,
  firebillRetryTotal,
  firebillTrackTotal,
} from "./metrics";

// firebill's own internal budget (durable write + forward attempt) is up to
// ~3.5s worst case, so this is deliberately looser than the 2s timeout on the
// direct Autumn client.
const FIREBILL_TIMEOUT_MS = 5000;

// Safe to retry because the idempotency key is stable across attempts: if the
// first attempt did land (ambiguous confirm timeout), Autumn dedupes the second.
// Small because a caller may be waiting, and a firebill refusing events usually
// cannot reach the broker — which more attempts will not fix.
const FIREBILL_ATTEMPTS = 2;
const FIREBILL_RETRY_DELAY_MS = 150;

// FIREBILL_ORG_IDS is decoded once at startup by the config schema; the Set is
// built lazily on first use and cached, keyed on the decoded array reference.
let allowlistCache: { source: string[] | undefined; ids: Set<string> } | null =
  null;

function firebillOrgIds(): Set<string> {
  const source = config.FIREBILL_ORG_IDS;
  if (!allowlistCache || allowlistCache.source !== source) {
    allowlistCache = {
      source,
      ids: new Set(
        (source ?? []).map(id => id.trim()).filter(id => id.length > 0),
      ),
    };
  }
  return allowlistCache.ids;
}

/**
 * Whether this org's usage goes through firebill rather than straight to Autumn.
 * Needs firebill configured, then either the allowlist or the sticky percentage.
 */
export function shouldRouteToFirebill(orgId: string): boolean {
  if (!config.FIREBILL_URL || !config.FIREBILL_SECRET) return false;
  // Always-on set: test orgs stay routed even at 0 percent.
  if (firebillOrgIds().has(orgId)) return true;
  // Sticky by org, so a ramp only ever adds and 0 is the kill switch.
  return sampled(orgId, config.FIREBILL_ROLLOUT_PERCENT);
}

/**
 * Sends a usage event to firebill, which publishes it to a durable quorum queue
 * and answers once the broker has confirmed it, then forwards it to Autumn from
 * a consumer (retrying failed deliveries instead of losing them).
 *
 * A negative value is a refund (refundCredits negates before calling track);
 * firebill's /v1/refund endpoint expects the POSITIVE amount and negates it
 * itself, so the absolute value is sent either way.
 *
 * Returns true when firebill accepted the event, mirroring the boolean
 * contract of the direct Autumn track path. A `false` here means "not billed
 * yet" — firebill keeps retrying delivery on its side.
 */
export async function firebillTrack(params: TrackParams): Promise<boolean> {
  const path = params.value < 0 ? "/v1/refund" : "/v1/track";

  for (let attempt = 1; attempt <= FIREBILL_ATTEMPTS; attempt++) {
    const result = await firebillAttempt(path, params);
    if (result.ok) {
      firebillTrackTotal.labels(path, "accepted").inc();
      return true;
    }

    if (attempt < FIREBILL_ATTEMPTS) {
      firebillRetryTotal.labels(result.reason).inc();
      await new Promise(resolve =>
        setTimeout(resolve, FIREBILL_RETRY_DELAY_MS),
      );
      continue;
    }

    // Nobody owns this event now, so the usage is gone unless someone acts on
    // it. A counter, not a throw: billing must not fail the customer's request,
    // and a log alone is too quiet to alert on.
    firebillTrackTotal.labels(path, "refused").inc();
    logger.error("firebill refused a usage event; it will not be billed", {
      customerId: params.customerId,
      entityId: params.entityId,
      featureId: params.featureId,
      value: params.value,
      idempotencyKey: params.idempotencyKey,
      path,
      attempts: FIREBILL_ATTEMPTS,
      reason: result.reason,
    });
    return false;
  }

  return false;
}

type AttemptResult =
  | { ok: true }
  | { ok: false; reason: "not_ok" | "not_success" | "exception" };

async function firebillAttempt(
  path: string,
  {
    customerId,
    entityId,
    featureId,
    value,
    properties,
    idempotencyKey,
  }: TrackParams,
): Promise<AttemptResult> {
  // Plain concatenation rather than new URL(path, base): a leading-slash path
  // would drop any base-path prefix (e.g. a reverse proxy at /firebill).
  const url = `${config.FIREBILL_URL!.replace(/\/+$/, "")}${path}`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${config.FIREBILL_SECRET}`,
        "content-type": "application/json",
      },
      // No overage flag needed: firebill itself pins Autumn's
      // overage_behavior to "overflow" on every upstream call, matching the
      // direct-Autumn path below.
      body: JSON.stringify({
        customer_id: customerId,
        entity_id: entityId,
        feature_id: featureId,
        value: Math.abs(value),
        properties,
        // firebill carries this to Autumn as the Idempotency-Key on every
        // attempt, so a requeued job re-billing the same work is deduped rather
        // than charged twice. Omitted → firebill mints a per-request UUID,
        // which dedupes only its own retries.
        ...(idempotencyKey ? { idempotency_key: idempotencyKey } : {}),
      }),
      signal: AbortSignal.timeout(FIREBILL_TIMEOUT_MS),
    });

    if (!response.ok) {
      logger.warn("firebill track attempt failed — non-OK response", {
        customerId,
        entityId,
        featureId,
        value,
        path,
        status: response.status,
      });
      return { ok: false, reason: "not_ok" };
    }

    const body = (await response.json()) as { success?: boolean };
    if (body.success !== true) {
      logger.warn("firebill track attempt did not succeed", {
        customerId,
        entityId,
        featureId,
        value,
        path,
      });
      return { ok: false, reason: "not_success" };
    }

    logger.info("firebill track succeeded", {
      customerId,
      entityId,
      featureId,
      value,
      path,
    });
    return { ok: true };
  } catch (error) {
    // DO NOT fall back to Autumn directly: firebill may have accepted the event
    // before this failed, and the Autumn SDK sends no idempotency key, so the
    // pair could not be deduped and the customer would be billed twice.
    logger.warn("firebill track attempt failed — firebill may be unavailable", {
      customerId,
      entityId,
      featureId,
      value,
      path,
      error,
    });
    return { ok: false, reason: "exception" };
  }
}
