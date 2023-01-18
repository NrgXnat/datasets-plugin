package org.nrg.xnatx.plugins.datasets.services.impl;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.nrg.framework.services.SerializerService;
import org.nrg.xapi.exceptions.DataFormatException;
import org.nrg.xapi.exceptions.InsufficientPrivilegesException;
import org.nrg.xapi.exceptions.NotFoundException;
import org.nrg.xapi.exceptions.ResourceAlreadyExistsException;
import org.nrg.xdat.om.SetsCollection;
import org.nrg.xft.event.XftItemEventI;
import org.nrg.xft.security.UserI;
import org.nrg.xnat.services.messaging.archive.AbstractResourceMitigationEventHandlerMethod;
import org.nrg.xnat.services.messaging.archive.ResourceMitigationEventProperties;
import org.nrg.xnatx.plugins.datasets.services.DatasetCollectionService;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
@Slf4j
public class DatasetsResourceMitigationEventHandlerMethod extends AbstractResourceMitigationEventHandlerMethod {
    public static final String PARAM_PATH                    = "path";
    public static final String QUERY_FIND_PATH_IN_COLLECTION = "SELECT id FROM sets_collection WHERE files LIKE :" + PARAM_PATH;

    private final DatasetCollectionService _service;

    public DatasetsResourceMitigationEventHandlerMethod(final DatasetCollectionService service, final SerializerService serializer, final NamedParameterJdbcTemplate template) {
        super(serializer, template);
        _service = service;
    }


    @Override
    protected boolean handleMitigationEvent(final ResourceMitigationEventProperties properties, final XftItemEventI event) {
        log.debug("Handling resource mitigation event requested by user {} with a total of {} moved or deleted files", properties.getUsername(), properties.getTotalFileCount());

        final UserI requester = properties.getRequester();

        final Map<String, List<String>> referencedDeletedFiles = properties.getDeleted().stream()
                                                                           .distinct()
                                                                           .map(this::getMatchingCollections)
                                                                           .filter(entry -> !entry.getValue().isEmpty())
                                                                           .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        final Map<String, List<String>> referencedMovedFiles = properties.getMoved().keySet().stream()
                                                                         .distinct()
                                                                         .map(this::getMatchingCollections)
                                                                         .filter(entry -> !entry.getValue().isEmpty())
                                                                         .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        if (referencedDeletedFiles.isEmpty() && referencedMovedFiles.isEmpty()) {
            log.debug("From a total of {} moved or deleted files, no realized dataset collections were affected", properties.getTotalFileCount());
            return true;
        }

        final Map<String, List<String>> distinctDeletedCollections = invertCollectionsAndPaths(referencedDeletedFiles);
        log.info("From a total of {} deleted files, {} files were contained in {} realized dataset collections: {}",
                 properties.getDeleted().size(),
                 referencedDeletedFiles.size(),
                 distinctDeletedCollections.size(),
                 String.join(", ", distinctDeletedCollections.keySet()));

        for (final String collectionId : distinctDeletedCollections.keySet()) {
            final List<String> paths = distinctDeletedCollections.get(collectionId);
            try {
                final SetsCollection collection = _service.findById(requester, collectionId);
                final JsonNode       oldFiles   = deserializeJson(collection.getFiles());

                // If the children are object nodes, the collection is something like [{"one": "/path/1", "two": "/path/2"},...]
                // which prevents us from deleting the nodes. If that's the case, log it and bail out.
                if (oldFiles.iterator().next().isObject()) {
                    log.warn("The collection with ID {} contains {} paths that have been deleted from the system, but the collection contains compound paths, so removing them will break the collection: {}", collectionId, paths.size(), String.join(", ", paths));
                    break;
                }

                final Map<Boolean, List<JsonNode>> nodes = StreamSupport.stream(Spliterators.spliteratorUnknownSize(oldFiles.iterator(), Spliterator.ORDERED), false).collect(Collectors.partitioningBy(JsonNode::isTextual));
                if (!nodes.get(false).isEmpty()) {
                    log.warn("Found {} nodes in collection {} that aren't pure text, these can't be brought back into the collection directly!", nodes.get(false).size(), collectionId);
                }

                final List<String> files = nodes.get(true).stream().map(JsonNode::asText).filter(path -> {
                    if (paths.contains(path)) {
                        log.info("Removing deleted path {} from collection {}", path, collectionId);
                        return false;
                    }
                    log.debug("Keeping path {} in collection {} (path was not deleted)", path, collectionId);
                    return true;
                }).collect(Collectors.toList());
                if (!files.isEmpty()) {
                    collection.setFiles(toJson(files));
                    _service.update(requester, collection);
                }
            } catch (NotFoundException e) {
                log.error("Got an event saying files were moved in collection {} but couldn't find a collection with that ID!", collectionId);
            } catch (InsufficientPrivilegesException e) {
                log.error("Files were moved in collection {} but couldn't update that collection with the user {}", collectionId, requester.getUsername(), e);
            } catch (DataFormatException e) {
                log.error("Files were moved in collection {} but got a data format error when trying to update that collection with the user {}", collectionId, requester.getUsername(), e);
            } catch (ResourceAlreadyExistsException e) {
                log.error("Files were moved in collection {} but got a resource already exists error when trying to update that collection with the user {}", collectionId, requester.getUsername(), e);
            } catch (IOException e) {
                log.error("Files were moved in collection {} but got an error when trying to deserialize that collection with the user {}", collectionId, requester.getUsername(), e);
            }
        }

        final Map<String, List<String>> distinctMovedCollections = invertCollectionsAndPaths(referencedMovedFiles);
        log.info("From a total of {} moved files, {} files were contained in {} realized dataset collections: {}", properties.getMoved().size(), referencedMovedFiles.size(), distinctMovedCollections.size(), String.join(", ", distinctMovedCollections.keySet()));

        for (final String collectionId : distinctMovedCollections.keySet()) {
            final Map<String, String> paths = distinctMovedCollections.get(collectionId).stream().collect(Collectors.toMap(Function.identity(), properties.getMoved()::get));
            final String[]            find  = paths.keySet().stream().map(path -> StringUtils.wrap(path, "\"")).toArray(String[]::new);
            try {
                final SetsCollection collection = _service.findById(requester, collectionId);
                collection.setFiles(StringUtils.replaceEachRepeatedly(collection.getFiles(), find, Arrays.stream(find).map(paths::get).map(path -> StringUtils.wrap(path, "\"")).toArray(String[]::new)));
                _service.update(requester, collection);
            } catch (NotFoundException e) {
                log.error("Got an event saying files were moved in collection {} but couldn't find a collection with that ID!", collectionId);
            } catch (InsufficientPrivilegesException e) {
                log.error("Files were moved in collection {} but couldn't update that collection with the user {}", collectionId, requester.getUsername(), e);
            } catch (DataFormatException e) {
                log.error("Files were moved in collection {} but got a data format error when trying to update that collection with the user {}", collectionId, requester.getUsername(), e);
            } catch (ResourceAlreadyExistsException e) {
                log.error("Files were moved in collection {} but got a resource already exists error when trying to update that collection with the user {}", collectionId, requester.getUsername(), e);
            }
        }
        return true;
    }

    private Pair<String, List<String>> getMatchingCollections(final String path) {
        return Pair.of(path, getTemplate().queryForList(QUERY_FIND_PATH_IN_COLLECTION, new MapSqlParameterSource(PARAM_PATH, "%\"" + path + "\"%"), String.class));
    }
}
