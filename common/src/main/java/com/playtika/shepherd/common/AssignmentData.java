package com.playtika.shepherd.common;

public record AssignmentData(
        long populationVersion,
        String memberId, int generation, boolean isLeader) {
}
