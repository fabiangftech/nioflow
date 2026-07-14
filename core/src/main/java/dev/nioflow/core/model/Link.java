package dev.nioflow.core.model;

import java.util.List;

public sealed interface Link
        permits Stage, AsyncStage, Decision, Recovery, Filter, Background, FanOut, Batch, Fork {

    List<Guard> guards();
}
