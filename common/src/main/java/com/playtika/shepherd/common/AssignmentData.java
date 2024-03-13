package com.playtika.shepherd.common;

public record AssignmentData(
        int assignmentSize, long populationVersion,
        String memberId, int generation, boolean isLeader) {
}
