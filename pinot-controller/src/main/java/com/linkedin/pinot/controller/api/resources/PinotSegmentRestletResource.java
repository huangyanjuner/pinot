/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.api.resources;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Current URI Mappings:
 * <ul>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}":
 *     "/tables/{tableName}/segments/{segmentName}/metadata":
 *     Get segment metadata for a given segment
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments":
 *     "/tables/{tableName}/segments/metadata":
 *     List segment metadata for a given table
 *   </li>
 *   <li>
 *      "/tables/{tableName}/segments/crc":
 *      Get crc information for a given table
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}?state={state}":
 *     Change the state of the segment to specified {state} (enable|disable|drop)
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments?state={state}":
 *     Change the state of all segments of the table to specified {state} (enable|disable|drop)
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}/reload":
 *     Reload the segment
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/reload":
 *     Reload all segments of the table
 *   </li>
 * </ul>
 *
 * {@inheritDoc}
 * @see org.restlet.resource.ServerResource#get()
 */

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotSegmentRestletResource {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);
  public static final Response.Status BAD_REQUEST = Response.Status.BAD_REQUEST;
  public static final Response.Status INTERNAL_ERROR = Response.Status.INTERNAL_SERVER_ERROR;

  private static final long _offlineToOnlineTimeoutInseconds = 5L;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Path("tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles segment states", notes = "Toggles segment states if 'state' is specified in query param, otherwise lists metadata")
  public String toggleStateOrListMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    StateType state = Constants.validateState(stateStr);

    if (stateStr == null) {
      JSONArray result = getAllSegmentsMetadataForTable(tableName, tableType);
      return result.toString();
    }
    return toggleStateInternal(tableName, state, tableType, null, _pinotHelixResourceManager).toString();
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles state of a segment", notes = "Toggles segment state if 'state' is specified in query param, otherwise lists segment metadata")
  public String toggleStateOrListMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "online|offline|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    StateType stateType = Constants.validateState(stateStr);
    if (stateStr == null) {
      // This is a list metadata operation
      return getSegmentMetaData(tableName, segmentName, tableType).toString();
    } else {
      return toggleStateInternal(tableName, stateType, tableType, segmentName, _pinotHelixResourceManager).toString();
    }
  }

  private String checkGetEncodedParam(String encoded) {
    try {
      return URLDecoder.decode(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      String errStr = "Could not decode parameter '" + encoded + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST);
    }
  }

  @GET
  @Path("tables/{tableName}/segments/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata all segments of table", notes = "Lists segment metadata")
  public String listMetadataForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    JSONArray result = getAllSegmentsMetadataForTable(tableName, tableType);
    return result.toString();
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata one segments of table", notes = "Lists segment metadata")
  public String listMetadataForOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    // The code in restlet.resources seems to return an array of arrays, so we will do the same
    // to maintain backward compatibility
    JSONArray result = new JSONArray();
    if (tableType != null) {
      JSONArray metadata = getSegmentMetaData(tableName, segmentName, tableType);
      result.put(metadata);
      return result.toString();
    }
    // Again,keeping backward compatibility, returning metadata from both table types.
    // The segment should appear only in one
    JSONArray metata;

    metata = getSegmentMetaData(tableName, segmentName, CommonConstants.Helix.TableType.OFFLINE);
    result.put(metata);
    metata = getSegmentMetaData(tableName, segmentName, CommonConstants.Helix.TableType.REALTIME);
    result.put(metata);
    return result.toString();
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads one segment", notes = "Reloads one segment")
  public String reloadOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return reloadSegmentForTable(tableName, segmentName, tableType);
  }

  @GET
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads all segments of a table", notes = "Reloads all segments")
  public String reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return reloadAllSegmentsForTable(tableName, tableType);
  }

  @GET
  @Path("tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets crc of all segments of a table", notes = "Gets crc of all segments")
  public String getCrcForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    return getAllCrcMetadataForTable(tableName);
  }

  //////////////////////////////////////////////////////////////////////////////////////

  private String reloadSegmentForTable(String tableName, String segmentName,
      CommonConstants.Helix.TableType tableType) {
    int numReloadMessagesSent = 0;

    if ((tableType == null) || CommonConstants.Helix.TableType.OFFLINE == tableType) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(offlineTableName, segmentName);
    }

    if ((tableType == null) || CommonConstants.Helix.TableType.REALTIME == tableType) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(realtimeTableName, segmentName);
    }

    return "Sent " + numReloadMessagesSent + " reload messages";
  }

  private String reloadAllSegmentsForTable(String tableName, @Nullable CommonConstants.Helix.TableType tableType) {
    int numReloadMessagesSent = 0;

    if ((tableType == null) || CommonConstants.Helix.TableType.OFFLINE == tableType) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(offlineTableName);
    }

    if ((tableType == null) || CommonConstants.Helix.TableType.REALTIME == tableType) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(realtimeTableName);
    }

    return "Sent " + numReloadMessagesSent + " reload messages";
  }

  public static JSONArray toggleStateInternal(String tableName, StateType state,
      CommonConstants.Helix.TableType tableType, String segmentName, PinotHelixResourceManager helixResourceManager) {
    JSONArray ret = new JSONArray();
    List<String> segmentsToToggle = new ArrayList<>();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String tableNameWithType = "";
    List<String> realtimeSegments = helixResourceManager.getSegmentsFor(realtimeTableName);
    List<String> offlineSegments = helixResourceManager.getSegmentsFor(offlineTableName);
    if (tableType == null) {
      PinotResourceManagerResponse responseRealtime =
          toggleSegmentsForTable(realtimeSegments, realtimeTableName, segmentName, state, helixResourceManager);
      PinotResourceManagerResponse responseOffline =
          toggleSegmentsForTable(offlineSegments, offlineTableName, segmentName, state, helixResourceManager);
      if (!responseOffline.isSuccessful() || !responseRealtime.isSuccessful()) {
        throw new WebApplicationException("OFFLINE response : " + responseOffline.getMessage() + ", REALTIME response"
            + responseRealtime.getMessage(), INTERNAL_ERROR);
      }
      List<PinotResourceManagerResponse> responses = new ArrayList<>();
      responses.add(responseRealtime);
      responses.add(responseOffline);
      ret.put(responses);
      return ret;
    } else if (CommonConstants.Helix.TableType.REALTIME == tableType) {
      if (helixResourceManager.hasRealtimeTable(tableName)) {
        tableNameWithType = realtimeTableName;
        if (segmentName != null) {
          segmentsToToggle = Collections.singletonList(segmentName);
        } else {
          segmentsToToggle.addAll(realtimeSegments);
        }
      } else {
        throw new WebApplicationException("There is no realtime table for " + tableName, BAD_REQUEST);
      }
    } else {
      if (helixResourceManager.hasOfflineTable(tableName)) {
        tableNameWithType = offlineTableName;
        if (segmentName != null) {
          segmentsToToggle = Collections.singletonList(segmentName);
        } else {
          segmentsToToggle.addAll(offlineSegments);
        }
      } else {
        throw new WebApplicationException("There is no offline table for: " + tableName, BAD_REQUEST);
      }
    }
    PinotResourceManagerResponse resourceManagerResponse =
        toggleSegmentsForTable(segmentsToToggle, tableNameWithType, segmentName, state, helixResourceManager);
    ret.put(resourceManagerResponse);
    return ret;
  }

  /**
   * Helper method to toggle state of segment for a given table. The tableName expected is the internally
   * stored name (with offline/realtime annotation).
   *
   * @param segmentsToToggle: segments that we want to perform operations on
   * @param tableName: Internal name (created by TableNameBuilder) for the table
   * @param segmentName: Segment to set the state for.
   * @param state: Value of state to set.
   * @return
   * @throws JSONException
   */
  private static PinotResourceManagerResponse toggleSegmentsForTable(@Nonnull List<String> segmentsToToggle,
      @Nonnull String tableName, String segmentName, @Nonnull StateType state,
      PinotHelixResourceManager helixResourceManager) {
    long timeOutInSeconds = 10L;
    if (segmentName == null) {
      // For enable, allow 5 seconds per segment for an instance as timeout.
      if (state == StateType.ENABLE) {
        int instanceCount = helixResourceManager.getAllInstances().size();
        if (instanceCount != 0) {
          timeOutInSeconds = (long) ((_offlineToOnlineTimeoutInseconds * segmentsToToggle.size()) / instanceCount);
        } else {
          return new PinotResourceManagerResponse("Error: could not find any instances in table " + tableName, false);
        }
      }
    }

    switch (state) {
      case ENABLE:
        return helixResourceManager.toggleSegmentState(tableName, segmentsToToggle, true, timeOutInSeconds);
      case DISABLE:
        return helixResourceManager.toggleSegmentState(tableName, segmentsToToggle, false, timeOutInSeconds);
      case DROP:
        return helixResourceManager.deleteSegments(tableName, segmentsToToggle);
      default:
        throw new WebApplicationException("Invalid state", BAD_REQUEST);
    }
  }

  private JSONArray getAllSegmentsMetadataForTable(String tableName, CommonConstants.Helix.TableType tableType) {
    boolean foundRealtimeTable = false;
    boolean foundOfflineTable = false;
    JSONArray ret = new JSONArray();
    try {
      if ((tableType == null || tableType == CommonConstants.Helix.TableType.REALTIME)
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        JSONObject realtime = new JSONObject();
        realtime.put(FileUploadPathProvider.TABLE_NAME, realtimeTableName);
        realtime.put("segments", new ObjectMapper().writeValueAsString(
            _pinotHelixResourceManager.getInstanceToSegmentsInATableMap(realtimeTableName)));
        ret.put(realtime);
        foundRealtimeTable = true;
      }

      if ((tableType == null
          || tableType == CommonConstants.Helix.TableType.OFFLINE && _pinotHelixResourceManager.hasOfflineTable(
          tableName))) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        JSONObject offline = new JSONObject();
        offline.put(FileUploadPathProvider.TABLE_NAME, offlineTableName);
        offline.put("segments", new ObjectMapper().writeValueAsString(
            _pinotHelixResourceManager.getInstanceToSegmentsInATableMap(offlineTableName)));
        ret.put(offline);
        foundOfflineTable = true;
      }
    } catch (JSONException | JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to convert segment metadata to json, table: %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    if (foundOfflineTable || foundRealtimeTable) {
      return ret;
    } else {
      throw new WebApplicationException("Table " + tableName + " not found.", Response.Status.NOT_FOUND);
    }
  }

  private JSONArray getSegmentMetaData(String tableName, String segmentName,
      CommonConstants.Helix.TableType tableType) {
    if (!ZKMetadataProvider.isSegmentExisted(_pinotHelixResourceManager.getPropertyStore(), tableName, segmentName)) {
      String error = new String("Error: segment " + segmentName + " not found.");
      LOGGER.info(error);
      throw new WebApplicationException(error, BAD_REQUEST);
    }
    try {
      JSONArray ret = new JSONArray();
      JSONObject jsonObj = new JSONObject();
      jsonObj.put(FileUploadPathProvider.TABLE_NAME, tableName);

      ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();

      if (tableType == tableType.OFFLINE) {
        OfflineSegmentZKMetadata offlineSegmentZKMetadata =
            ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableName, segmentName);
        jsonObj.put(FileUploadPathProvider.STATE, offlineSegmentZKMetadata.toMap());
      }

      if (tableType == CommonConstants.Helix.TableType.REALTIME) {
        RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
            ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableName, segmentName);
        jsonObj.put(FileUploadPathProvider.STATE, realtimeSegmentZKMetadata.toMap());
      }
      ret.put(jsonObj);
      return ret;
    } catch (JSONException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid json for segment metadata, table: %s, segment: %s, type: %s", tableName, segmentName,
              tableType), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private String getAllCrcMetadataForTable(String tableName) {
    PinotResourceManagerResponse response;

    // TODO
    // In the restlet.resource version, we see this code block below
    // seems to be wrong comparing the table name to have the table type, but we copy it here anyway.
    // Realtime table is not supported.
    if (TableNameBuilder.getTableTypeFromTableName(tableName) == CommonConstants.Helix.TableType.REALTIME) {
      throw new WebApplicationException("Realtime table is not supported", Response.Status.FORBIDDEN);
    }

    // Check that the offline table exists.
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (!_pinotHelixResourceManager.getAllTables().contains(offlineTableName)) {
      throw new WebApplicationException("Offline table " + tableName + " does not exist.", BAD_REQUEST);
    }

    Map<String, String> segmentCrcForTable = _pinotHelixResourceManager.getSegmentsCrcForTable(offlineTableName);
    ObjectMapper mapper = new ObjectMapper();
    String result = null;
    try {
      result = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(segmentCrcForTable);
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to write segment crc values for table: %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return result;
  }
}
