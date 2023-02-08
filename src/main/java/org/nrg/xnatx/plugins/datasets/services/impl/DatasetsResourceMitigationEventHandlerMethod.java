package org.nrg.xnatx.plugins.datasets.services.impl;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringSubstitutor;
import org.nrg.framework.services.SerializerService;
import org.nrg.mail.services.MailService;
import org.nrg.xapi.exceptions.NotFoundException;
import org.nrg.xdat.preferences.NotificationsPreferences;
import org.nrg.xft.event.XftItemEventI;
import org.nrg.xft.security.UserI;
import org.nrg.xnat.entities.ResourceSurveyRequest;
import org.nrg.xnat.services.archive.ResourceSurveyRequestEntityService;
import org.nrg.xnat.services.messaging.archive.AbstractResourceMitigationEventHandlerMethod;
import org.nrg.xnat.services.messaging.archive.ResourceMitigationEventProperties;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import javax.mail.MessagingException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Slf4j
public class DatasetsResourceMitigationEventHandlerMethod extends AbstractResourceMitigationEventHandlerMethod {
    public static final String PARAM_PATH                          = "path";
    public static final String PARAM_REQUEST_ID                    = "requestId";
    public static final String PARAM_COLLECTION_IDS                = "collectionIds";
    public static final String QUERY_FIND_PATH_IN_COLLECTION       = "SELECT id FROM sets_collection WHERE files ~ :" + PARAM_PATH;
    public static final String QUERY_GET_COLLECTION_IDS_AND_LABELS = "SELECT c.id AS collection_id, "
                                                                     + "       xc.label AS collection_label "
                                                                     + "FROM sets_collection c "
                                                                     + "         LEFT JOIN xnat_experimentdata xc ON c.id = xc.id "
                                                                     + "         LEFT JOIN sets_definition d ON c.definition_id = d.id "
                                                                     + "         LEFT JOIN xnat_experimentdata xd ON d.id = xd.id "
                                                                     + "WHERE c.id IN (:" + PARAM_COLLECTION_IDS + ")";
    public static final String QUERY_FIND_COLLECTION_OWNERS        = "WITH projects AS (SELECT DISTINCT project "
                                                                     + "                  FROM sets_collection sc "
                                                                     + "                           LEFT JOIN xnat_experimentdata x ON sc.id = x.id "
                                                                     + "                  WHERE sc.id IN (:collectionIds)), "
                                                                     + "     group_owners AS (SELECT u.email AS owner "
                                                                     + "                      FROM xdat_usergroup g "
                                                                     + "                               LEFT JOIN xdat_user_groupid ug ON g.id = ug.groupid "
                                                                     + "                               LEFT JOIN xdat_user u ON ug.groups_groupid_xdat_user_xdat_user_id = u.xdat_user_id "
                                                                     + "                      WHERE g.tag IN (SELECT project FROM projects) "
                                                                     + "                        AND g.id ~ '^.*_owner$'), "
                                                                     + "     expt_owners AS (SELECT u.email AS owner "
                                                                     + "                     FROM sets_collection sc "
                                                                     + "                              LEFT JOIN sets_collection_meta_data scmd ON sc.collection_info = scmd.meta_data_id "
                                                                     + "                              LEFT JOIN xdat_user u ON scmd.insert_user_xdat_user_id = u.xdat_user_id "
                                                                     + "                     WHERE sc.id IN (:collectionIds)), "
                                                                     + "     owners AS (SELECT owner FROM group_owners UNION SELECT owner FROM expt_owners) "
                                                                     + "SELECT DISTINCT owner "
                                                                     + "FROM owners";
    public static final String SUBJECT_AFFECTED_COLLECTIONS        = "Dataset collection files moved and/or deleted";
    public static final String EMAIL_AFFECTED_COLLECTIONS          = "<p>The user <a href=\"mailto:USER_EMAIL\">USER_USERNAME</a> recently ran the DICOM resource survey and mitigation workflow on your "
                                                                     + "\"SITE_NAME\" XNAT deployment at SITE_LINK. As a result files were renamed or deleted in a number of DICOM resource folders, "
                                                                     + "including one or more that were referenced in one or more dataset collections:</p>"
                                                                     + "<ul><li>${" + PARAM_COLLECTION_IDS + "}</li></ul>"
                                                                     + "<p>The <a href=\"SITE_URL/xapi/resources/mitigate/request/${" + PARAM_REQUEST_ID + "}/report\">resource survey request mitigation report</a> "
                                                                     + "may be helpful for determining whether your collections are still valid or need to be regenerated.</p>";

    private static final String COLLECTION_LINK = "<a href=\"SITE_URL/data/experiments/%s\">%s</a>";

    private final ResourceSurveyRequestEntityService _entityService;
    private final NotificationsPreferences           _preferences;
    private final MailService                        _mailService;

    public DatasetsResourceMitigationEventHandlerMethod(final ResourceSurveyRequestEntityService entityService, final NotificationsPreferences preferences, final MailService mailService, final SerializerService serializer, final NamedParameterJdbcTemplate template) {
        super(serializer, template);
        _entityService = entityService;
        _preferences   = preferences;
        _mailService   = mailService;
    }

    @Override
    protected boolean handleMitigationEvent(final ResourceMitigationEventProperties properties, final XftItemEventI event) {
        log.debug("Handling resource mitigation event requested by user {} with a total of {} moved or deleted files", properties.getUsername(), properties.getTotalFileCount());

        final ResourceSurveyRequest request = properties.getRequest();

        try {
            final List<ResourceSurveyRequest> requests = _entityService.getOpenRequestsByRequestTime(request.getRequestTime());
            if (requests.size() > 0) {
                log.info("Received resource survey request {} with request time {} but there are still {} open requests with that request time. Waiting until all are completed.", request.getId(), request.getRequestTime(), requests.size());
                return true;
            }
        } catch (NotFoundException e) {
            log.error("I tried to retrieve open resource survey requests by request time {} but no requests were found with that request time, which is REALLY weird because I actually have one right now with request {}, so I don't know what to do. Skipping.", request.getRequestTime(), request.getId());
            return false;
        }

        log.info("Received resource survey request {} with request time {} and no more open requests with that request time. Checking for affected dataset collections.", request.getId(), request.getRequestTime());

        final List<ResourceSurveyRequest> requests;
        try {
            requests = _entityService.getAllRequestsByRequestTime(request.getRequestTime());
        } catch (NotFoundException e) {
            log.error("I tried to retrieve all resource survey requests by request time {} but no requests were found with that request time, which is REALLY weird because I actually have one right now with request {}, so I don't know what to do. Skipping.", request.getRequestTime(), request.getId());
            return false;
        }

        final Set<String> collectionIds = requests.stream().map(this::getAffectedCollectionIds).flatMap(Collection::stream).collect(Collectors.toSet());
        log.debug("From a set of {} requests with request time {} I found {} affected collections", requests.size(), request.getRequestTime(), collectionIds.size());

        final UserI requester = properties.getRequester();
        final List<String> links = getTemplate().query(QUERY_GET_COLLECTION_IDS_AND_LABELS, new MapSqlParameterSource(PARAM_COLLECTION_IDS, collectionIds), resultSet -> {
            final List<String> list = new ArrayList<>();
            while (resultSet.next()) {
                list.add(String.format(COLLECTION_LINK, resultSet.getString("collection_id"), resultSet.getString("collection_label")));
            }
            return list;
        });
        final String[] tos = getTemplate().queryForList(QUERY_FIND_COLLECTION_OWNERS, new MapSqlParameterSource(PARAM_COLLECTION_IDS, collectionIds), String.class).toArray(new String[0]);
        final String body = _preferences.replaceCommonAnchorTags(StringSubstitutor.replace(EMAIL_AFFECTED_COLLECTIONS,
                                                                                           ImmutableMap.of(PARAM_REQUEST_ID, request.getId(),
                                                                                                           PARAM_COLLECTION_IDS, String.join("</li><li>", links))), requester);
        try {
            _mailService.sendHtmlMessage(requester.getEmail(), tos, ArrayUtils.EMPTY_STRING_ARRAY, ArrayUtils.EMPTY_STRING_ARRAY, SUBJECT_AFFECTED_COLLECTIONS, body);
        } catch (MessagingException e) {
            log.error("An error occurred trying to send an email to emails: {}\nBody: {}", String.join(", ", tos), body, e);
        }

        return true;
    }

    private Set<String> getAffectedCollectionIds(final ResourceSurveyRequest request) {
        final Map<File, File> removedFiles = request.getMitigationReport().getRemovedFiles();
        final Map<File, File> movedFiles   = request.getMitigationReport().getMovedFiles();
        final Map<String, List<String>> referencedDeletedFiles = removedFiles.keySet().stream().map(Objects::toString)
                                                                             .distinct()
                                                                             .map(this::getMatchingCollections)
                                                                             .filter(entry -> !entry.getValue().isEmpty())
                                                                             .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        final Map<String, List<String>> referencedMovedFiles = movedFiles.keySet().stream().map(Objects::toString)
                                                                         .distinct()
                                                                         .map(this::getMatchingCollections)
                                                                         .filter(entry -> !entry.getValue().isEmpty())
                                                                         .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        if (referencedDeletedFiles.isEmpty() && referencedMovedFiles.isEmpty()) {
            log.debug("From a total of {} moved files and {} deleted files from resource survey request {} for resource {},, no realized dataset collections were affected", movedFiles.size(), removedFiles.size(), request.getId(), request.getResourceId());
            return Collections.emptySet();
        }

        final Map<String, List<String>> distinctDeletedCollections = invertMapOfLists(referencedDeletedFiles);
        final Map<String, List<String>> distinctMovedCollections   = invertMapOfLists(referencedMovedFiles);
        log.debug("From a total of {} moved files and {} deleted files from resource survey request {} for resource {}, {} files were contained in {} realized dataset collections: {}",
                  movedFiles.size(),
                  removedFiles.size(),
                  request.getId(),
                  request.getResourceId(),
                  referencedMovedFiles.size() + referencedDeletedFiles.size(),
                  distinctMovedCollections.size() + distinctDeletedCollections.size(),
                  Stream.concat(distinctMovedCollections.keySet().stream(), distinctDeletedCollections.keySet().stream()).distinct().sorted().collect(Collectors.joining(", ")));

        return Stream.concat(distinctDeletedCollections.keySet().stream(), distinctMovedCollections.keySet().stream()).collect(Collectors.toSet());
    }

    private Pair<String, List<String>> getMatchingCollections(final String path) {
        return Pair.of(path, getTemplate().queryForList(QUERY_FIND_PATH_IN_COLLECTION, new MapSqlParameterSource(PARAM_PATH, "^.*[\\[ ]" + path + ",.*$"), String.class));
    }
}
