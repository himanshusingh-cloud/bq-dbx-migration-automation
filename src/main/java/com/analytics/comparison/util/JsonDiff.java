package com.analytics.comparison.util;

import lombok.Getter;

/**
 * Represents a single difference between two JSON values at a given path.
 * prod = production value, test = test environment value.
 */
@Getter
public class JsonDiff {
    private final String path;
    private final String prod;
    private final String test;

    public JsonDiff(String path, String prod, String test) {
        this.path = path;
        this.prod = prod;
        this.test = test;
    }

    /** @deprecated Use getProd/getTest */
    public String getExpected() { return prod; }
    /** @deprecated Use getProd/getTest */
    public String getActual() { return test; }

    @Override
    public String toString() {
        return String.format("path=%s | prod=%s | test=%s", path, prod, test);
    }
}
