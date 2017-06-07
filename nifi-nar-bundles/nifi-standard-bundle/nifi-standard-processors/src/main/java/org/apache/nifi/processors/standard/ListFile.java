/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.CanReadFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.FileInfo;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"file", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from the local filesystem. For each file that is listed, " +
        "creates a FlowFile that represents the file so that it can be fetched in conjunction with FetchFile. This " +
        "Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new " +
        "Primary Node will pick up where the previous node left off without duplicating all of the data. Unlike " +
        "GetFile, this Processor does not delete any data from the local filesystem.")
@WritesAttributes({
        @WritesAttribute(attribute="filename", description="The name of the file that was read from filesystem."),
        @WritesAttribute(attribute="path", description="The path is set to the relative path of the file's directory " +
                "on filesystem compared to the Input Directory property.  For example, if Input Directory is set to " +
                "/tmp, then files picked up from /tmp will have the path attribute set to \"/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"abc/1/2/3/\"."),
        @WritesAttribute(attribute="absolute.path", description="The absolute.path is set to the absolute path of " +
                "the file's directory on filesystem. For example, if the Input Directory property is set to /tmp, " +
                "then files picked up from /tmp will have the path attribute set to \"/tmp/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"/tmp/abc/1/2/3/\"."),
        @WritesAttribute(attribute=ListFile.FILE_OWNER_ATTRIBUTE, description="The user that owns the file in filesystem"),
        @WritesAttribute(attribute=ListFile.FILE_GROUP_ATTRIBUTE, description="The group that owns the file in filesystem"),
        @WritesAttribute(attribute=ListFile.FILE_SIZE_ATTRIBUTE, description="The number of bytes in the file in filesystem"),
        @WritesAttribute(attribute=ListFile.FILE_PERMISSIONS_ATTRIBUTE, description="The permissions for the file in filesystem. This " +
                "is formatted as 3 characters for the owner, 3 for the group, and 3 for other users. For example " +
                "rw-rw-r--"),
        @WritesAttribute(attribute=ListFile.FILE_TYPE_ATTRIBUTE, description="The file type, 'file' or 'directory', of the path " +
                "represented in the FlowFile"),
        @WritesAttribute(attribute=ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute=ListFile.FILE_LAST_ACCESS_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "last accessed as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute=ListFile.FILE_CREATION_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "created as 'yyyy-MM-dd'T'HH:mm:ssZ'")
})
@SeeAlso({GetFile.class, PutFile.class, FetchFile.class})
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. Whether the state is stored with a Local or Cluster scope depends on the value of the "
    + "<Input Directory Location> property.")
public class ListFile extends AbstractListProcessor<FileInfo> {
    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local", "Input Directory is located on a local disk. State will be stored locally on each node in the cluster.");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote", "Input Directory is located on a remote system. State will be stored across the cluster so that "
        + "the listing can be performed on Primary Node Only and another node can pick up where the last node left off, if the Primary Node changes");

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which files to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor MAX_DEPTH = new PropertyDescriptor.Builder()
            .name("max-depth")
            .displayName("Maximum Recursion Depth")
            .description("The maximum number of levels to descend in the " +
                "directory tree if recursion is enabled")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIRECTORY_LOCATION = new PropertyDescriptor.Builder()
            .name("Input Directory Location")
            .description("Specifies where the Input Directory is located. This is used to determine whether state should be stored locally or across the cluster.")
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();


    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
            .name("Maximum File Age")
            .description("The maximum age that a file must be in order to be pulled; any file older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();

    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
            .name("Minimum File Size")
            .description("The minimum size that a file must be in order to be pulled")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("0 B")
            .build();

    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum File Size")
            .description("The maximum size that a file can be in order to be pulled")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_TYPE_ATTRIBUTE = "file.type";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DIRECTORY);
        properties.add(RECURSE);
        properties.add(MAX_DEPTH);
        properties.add(DIRECTORY_LOCATION);
        properties.add(FILE_FILTER);
        properties.add(PATH_FILTER);
        properties.add(MIN_AGE);
        properties.add(MAX_AGE);
        properties.add(MIN_SIZE);
        properties.add(MAX_SIZE);
        properties.add(IGNORE_HIDDEN_FILES);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();

        final String fullPath = fileInfo.getFullPathFileName();
        final File file = new File(fullPath);
        final Path filePath = file.toPath();
        final Path directoryPath = new File(getPath(context)).toPath();

        final Path relativePath = directoryPath.toAbsolutePath().relativize(filePath.getParent());
        String relativePathString = relativePath.toString();
        relativePathString = relativePathString.isEmpty() ? "." + File.separator : relativePathString + File.separator;

        final Path absPath = filePath.toAbsolutePath();
        final String absPathString = absPath.getParent().toString() + File.separator;

        attributes.put(CoreAttributes.PATH.key(), relativePathString);
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);

        try {
            final Path realPath = filePath.toRealPath();
            if (Files.isDirectory(realPath)) {
                attributes.put(FILE_TYPE_ATTRIBUTE, "directory");
            } else if (Files.isRegularFile(realPath)) {
                attributes.put(FILE_TYPE_ATTRIBUTE, "file");
            }
        } catch (final IOException ioe) {
            throw new ProcessException(ioe.getMessage(), ioe);
        }

        try {
            FileStore store = Files.getFileStore(filePath);
            if (store.supportsFileAttributeView("basic")) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    BasicFileAttributeView view = Files.getFileAttributeView(filePath, BasicFileAttributeView.class);
                    BasicFileAttributes attrs = view.readAttributes();
                    attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(attrs.size()));
                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("owner")) {
                try {
                    FileOwnerAttributeView view = Files.getFileAttributeView(filePath, FileOwnerAttributeView.class);
                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("posix")) {
                try {
                    PosixFileAttributeView view = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
        } catch (IOException ioe) {
            // well then this FlowFile gets none of these attributes
            getLogger().warn("Error collecting attributes for file {}, message is {}",
                    new Object[]{absPathString, ioe.getMessage()});
        }

        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        final String filePath = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(filePath)) {
            return new File(filePath).getAbsolutePath();
        }
        return null;
    }

    @Override
    protected String getPath(final ProcessContext context, final FlowFile flowFile) {
        final ComponentLog logger = getLogger();

        if (flowFile != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Getting path from FlowFile: {}", new Object[]{flowFile});
            }

            final String fileType = flowFile.getAttribute(FILE_TYPE_ATTRIBUTE);
            if (logger.isDebugEnabled()) {
                logger.debug("File type for FlowFile {}: {}", new Object[]{flowFile, fileType});
            }

            if (StringUtils.isNotBlank(fileType) && fileType.equals("directory")) {
                final String filePath = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
                if (StringUtils.isNotBlank(filePath)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Path for FlowFile {}: {}", new Object[]{flowFile, filePath});
                    }
                    return new File(filePath).getAbsolutePath();
                } else {
                    logger.warn("Path for FlowFile {} was blank", new Object[]{flowFile});
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("File type for FlowFile {} was blank or non-directory, FlowFile may not be processed", new Object[]{flowFile});
            }
        }

        return null;
    }

    @Override
    protected Scope getStateScope(final ProcessContext context) {
        final String location = context.getProperty(DIRECTORY_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            return Scope.CLUSTER;
        }

        return Scope.LOCAL;
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        return performListing(context, null, minTimestamp);
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final FlowFile flowFile, final Long minTimestamp) throws IOException {
        final File path = new File(flowFile != null ? getPath(context, flowFile) : getPath(context));
        final ListFileDirectoryWalker walker = ListFileDirectoryWalker.newInstance(context, minTimestamp != null ? OptionalLong.of(minTimestamp) : OptionalLong.empty());
        return walker.listFiles(path);
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return DIRECTORY.equals(property)
                || RECURSE.equals(property)
                || FILE_FILTER.equals(property)
                || PATH_FILTER.equals(property)
                || MIN_AGE.equals(property)
                || MAX_AGE.equals(property)
                || MIN_SIZE.equals(property)
                || MAX_SIZE.equals(property)
                || IGNORE_HIDDEN_FILES.equals(property);
    }

    private static class ListFileDirectoryWalker extends DirectoryWalker<FileInfo> {

        private FileFilter fileFilter;

        private int maxDepth;

        private ListFileDirectoryWalker(final IOFileFilter dirFilter, final IOFileFilter fileFilter, final int maxDepth) {
            super(dirFilter, fileFilter, maxDepth);
            this.fileFilter = fileFilter;
            this.maxDepth = maxDepth;
        }

        protected boolean handleDirectory(final File directory, final int depth, final Collection<FileInfo> results) {
            if (maxDepth == -1 || (maxDepth > 0 && depth == 0)) {
                return true;
            }

            if (maxDepth >= 0 && depth <= maxDepth) {
                final OptionalLong lastModified = treeLastModified(directory);
                if (lastModified.isPresent()) {
                    if (depth == maxDepth) {
                        final FileInfo fileInfo = new FileInfo.Builder()
                                .directory(directory.isDirectory())
                                .filename(directory.getName())
                                .fullPathFileName(directory.getAbsolutePath())
                                .lastModifiedTime(lastModified.getAsLong() > directory.lastModified() ? lastModified.getAsLong() : directory.lastModified())
                                .build();
                        results.add(fileInfo);
                        return false;
                    }
                    return true;
                }
            }

            return false;
        }

        protected void handleFile(File file, int depth, Collection<FileInfo> results) {
            final FileInfo fileInfo = new FileInfo.Builder()
                    .directory(file.isDirectory())
                    .filename(file.getName())
                    .fullPathFileName(file.getAbsolutePath())
                    .lastModifiedTime(file.lastModified())
                    .build();
            results.add(fileInfo);
        }

        public List<FileInfo> listFiles(final File path) throws IOException {
            final List<FileInfo> results = new ArrayList<>();
            walk(path, results);
            return Collections.unmodifiableList(results);
        }

        private OptionalLong treeLastModified(final File path) {
            final File[] files = path.listFiles(fileFilter);
            Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_REVERSE);

            final List<File> directories = new ArrayList<>();
            for (final File file : files) {
                if (file.isFile()) {
                    return OptionalLong.of(file.lastModified());
                } else if (file.isDirectory()) {
                    directories.add(file);
                }
            }

            for (final File directory : directories) {
                final OptionalLong lastModified = treeLastModified(directory);
                if (lastModified.isPresent()) {
                    return lastModified;
                }
            }

            return OptionalLong.empty();
        }

        public static ListFileDirectoryWalker newInstance(final ProcessContext context, final OptionalLong minTimestamp) {
            final long minSize = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
            final Double maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
            final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            final Long maxAge = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
            final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
            final String indir = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
            final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
            final Integer maxDepth = context.getProperty(MAX_DEPTH).asInteger();
            final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
            final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);

            final List<IOFileFilter> fileFilters = new ArrayList<>();
            final int depth;

            fileFilters.add(FileFilterUtils.sizeFileFilter(minSize));
            if (maxSize != null) {
                fileFilters.add(FileFilterUtils.notFileFilter(FileFilterUtils.sizeFileFilter(maxSize.longValue())));
            }

            final long currentTimeMillis = System.currentTimeMillis();

            fileFilters.add(FileFilterUtils.ageFileFilter(currentTimeMillis - minAge));
            if (maxAge != null) {
                fileFilters.add(FileFilterUtils.notFileFilter(FileFilterUtils.ageFileFilter(currentTimeMillis - maxAge)));
            }

            if (ignoreHidden) {
                fileFilters.add(HiddenFileFilter.VISIBLE);
            }

            if (recurseDirs) {
                depth = maxDepth != null ? maxDepth : -1;
            } else {
                depth = 1;
            }

            if (pathPattern != null) {
                fileFilters.add(FileFilterUtils.asFileFilter(new FileFilter() {
                      @Override
                      public boolean accept(final File file) {
                          Path reldir = Paths.get(indir).relativize(file.toPath()).getParent();
                          if (reldir != null && !reldir.toString().isEmpty()) {
                              if (!pathPattern.matcher(reldir.toString()).matches()) {
                                  return false;
                              }
                          }
                          return true;
                      }
                }));
            }

            fileFilters.add(CanReadFileFilter.CAN_READ);

            fileFilters.add(FileFilterUtils.asFileFilter(new FileFilter() {
                  @Override
                  public boolean accept(final File file) {
                      return filePattern.matcher(file.getName()).matches();
                  }
            }));

            if (minTimestamp.isPresent()) {
                fileFilters.add(FileFilterUtils.notFileFilter(FileFilterUtils.ageFileFilter(minTimestamp.getAsLong())));
            }

            final IOFileFilter dirFilter = recurseDirs ? TrueFileFilter.INSTANCE : FalseFileFilter.INSTANCE;
            final IOFileFilter fileFilter = FileFilterUtils.or(new AndFileFilter(fileFilters), DirectoryFileFilter.DIRECTORY);

            return new ListFileDirectoryWalker(dirFilter, fileFilter, depth);
        }

    }

}
