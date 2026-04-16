package com.trading.engine.orchestrator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method, field, or constructor whose visibility has been intentionally widened beyond what
 * production callers require, in order to support unit testing. Production code in the same package
 * MUST NOT call these members; reviewers should treat any such call as a defect.
 *
 * <p>Project-local equivalent of Guava's {@code @VisibleForTesting}; defined here to avoid a Guava
 * dependency (CLAUDE.md mandates a tight dependency footprint for the trading engine).
 *
 * <p><b>Retention:</b> {@code SOURCE} — the annotation is intended as a marker for code reviewers
 * and IDE tooling, not for runtime introspection. Compiled bytecode does not carry it.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE, ElementType.CONSTRUCTOR})
public @interface VisibleForTesting {}
