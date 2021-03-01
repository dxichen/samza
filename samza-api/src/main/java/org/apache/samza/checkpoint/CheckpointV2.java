/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.checkpoint;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Objects;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;

/**
 * A checkpoint is a mapping of all the streams a job is consuming and the most recent current offset for each.
 * It is used to restore a {@link org.apache.samza.task.StreamTask}, either as part of a job restart or as part
 * of restarting a failed container within a running job.
 */

public class CheckpointV2 implements Checkpoint {
  public static final short CHECKPOINT_VERSION = 2;

  private final CheckpointId checkpointId;
  private final Map<SystemStreamPartition, String> inputOffsets;
  private final Map<String, List<StateCheckpointMarker>> stateCheckpointMarkers;

  /**
   * Constructs the checkpoint with separated input and state offsets
   * @param checkpointId CheckpointId associated with this checkpoint
   * @param inputOffsets Map of Samza system stream partition to offset of the checkpoint
   * @param stateCheckpointMarkers Map of local state store names and StateCheckpointMarkers for each state backend system
   */
  public CheckpointV2(CheckpointId checkpointId,
      Map<SystemStreamPartition, String> inputOffsets,
      Map<String, List<StateCheckpointMarker>> stateCheckpointMarkers) {
    this.checkpointId = checkpointId;
    this.inputOffsets = ImmutableMap.copyOf(inputOffsets);
    this.stateCheckpointMarkers = ImmutableMap.copyOf(stateCheckpointMarkers);
  }

  public short getVersion() {
    return CHECKPOINT_VERSION;
  }

  /**
   * Gets the checkpoint id for the checkpoint
   * @return The timestamp based checkpoint identifier associated with the checkpoint
   */
  public CheckpointId getCheckpointId() {
    return checkpointId;
  }

  /**
   * Gets a unmodifiable view of the current Samza stream offsets.
   * @return A unmodifiable view of a Map of Samza streams to their recorded offsets.
   */
  public Map<SystemStreamPartition, String> getInputOffsets() {
    return inputOffsets;
  }

  /**
   * Gets the stateCheckpointMarkers
   * @return The state checkpoint markers for the checkpoint
   */
  public Map<String, List<StateCheckpointMarker>> getStateCheckpointMarkers() {
    return stateCheckpointMarkers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CheckpointV2)) return false;

    CheckpointV2 that = (CheckpointV2) o;

    return checkpointId.equals(that.checkpointId) &&
        Objects.equals(inputOffsets, that.inputOffsets) &&
        Objects.equals(stateCheckpointMarkers, that.stateCheckpointMarkers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(checkpointId, inputOffsets, stateCheckpointMarkers);
  }

  @Override
  public String toString() {
    return "CheckpointV2 [SCHEMA_VERSION=" + CHECKPOINT_VERSION + ", checkpointId=" + checkpointId +
        ", inputOffsets=" + inputOffsets + ", stateCheckpoint=" + stateCheckpointMarkers + "]";
  }
}
