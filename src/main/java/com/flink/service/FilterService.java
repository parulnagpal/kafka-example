package com.flink.service;

public interface FilterService<I,O> {

    O filter(I item);
}
