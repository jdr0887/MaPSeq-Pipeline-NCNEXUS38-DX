package edu.unc.mapseq.workflow.ncnexus38.dx;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncnexus38.dx.RegisterToIRODSRunnable;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.core.ZipCLI;
import edu.unc.mapseq.module.sequencing.converter.SAMToolsDepthToGATKDOCFormatConverterCLI;
import edu.unc.mapseq.module.sequencing.filter.FilterVariantCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardSortOrderType;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetricsCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardSortSAMCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardViewSAMCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.core.WorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class NCNEXUS38DXWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUS38DXWorkflow.class);

    public NCNEXUS38DXWorkflow() {
        super();
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;
        String listVersion = null;
        String dxId = null;

        Set<Sample> sampleSet = SequencingWorkflowUtil.getAggregatedSamples(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                getWorkflowRunAttempt());
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String subjectMergeHome = getWorkflowBeanService().getAttributes().get("subjectMergeHome");

        Boolean isIncidental = Boolean.FALSE;
        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        Set<Attribute> attributeSet = workflowRun.getAttributes();
        if (CollectionUtils.isNotEmpty(attributeSet)) {
            Iterator<Attribute> attributeIter = attributeSet.iterator();
            while (attributeIter.hasNext()) {
                Attribute attribute = attributeIter.next();
                if ("list_version".equals(attribute.getName())) {
                    listVersion = attribute.getValue();
                }
                if ("dx_id".equals(attribute.getName())) {
                    dxId = attribute.getValue();
                }
            }
        }

        if (listVersion == null || dxId == null) {
            throw new WorkflowException("Both version and DX were null...returning empty dag");
        }

        Set<String> subjectNameSet = new HashSet<String>();
        for (Sample sample : sampleSet) {
            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }
            Set<Attribute> sampleAttributes = sample.getAttributes();
            Optional<Attribute> foundAttribute = sampleAttributes.stream().filter(a -> "subjectName".equals(a.getName())).findFirst();
            if (foundAttribute.isPresent()) {
                subjectNameSet.add(foundAttribute.get().getValue());
            }
        }

        Set<String> synchronizedSubjectNameSet = Collections.synchronizedSet(subjectNameSet);

        if (synchronizedSubjectNameSet.isEmpty()) {
            throw new WorkflowException("subjectNameSet is empty");
        }

        if (synchronizedSubjectNameSet.size() > 1) {
            throw new WorkflowException("multiple subjectName values across samples");
        }

        String subjectName = synchronizedSubjectNameSet.iterator().next();

        if (StringUtils.isEmpty(subjectName)) {
            throw new WorkflowException("empty subjectName");
        }

        String dataDirectory = System.getenv("MAPSEQ_DATA_DIRECTORY");
        if (StringUtils.isEmpty(dataDirectory)) {
            dataDirectory = "/projects/mapseq/data";
        }

        File allIntervalsFile = new File(
                String.format("%1$s/resources/annotation/abeast/NCNEXUS38/all/allintervals.v%2$s.txt", dataDirectory, listVersion));
        if (!allIntervalsFile.exists()) {
            throw new WorkflowException("allIntervalsFile does not exist: " + allIntervalsFile.getAbsolutePath());
        }

        File versionedExonsIntervalListFile = new File(String
                .format("%1$s/resources/annotation/abeast/NCNEXUS38/%2$s/exons_pm_0_v%2$s.interval_list", dataDirectory, listVersion));
        if (!versionedExonsIntervalListFile.exists()) {
            throw new WorkflowException("Interval list file does not exist: " + versionedExonsIntervalListFile.getAbsolutePath());
        }

        File versionedExonsBedFile = new File(
                String.format("%1$s/resources/annotation/abeast/NCNEXUS38/%2$s/exons_pm_0_v%2$s.bed", dataDirectory, listVersion));
        if (!versionedExonsBedFile.exists()) {
            throw new WorkflowException("BED file does not exist: " + versionedExonsBedFile.getAbsolutePath());
        }

        File intervalListByDXAndVersionFile = new File(String.format(
                "%1$s/resources/annotation/abeast/NCNEXUS38/%2$s/genes_dxid_%3$s_v_%2$s.interval_list", dataDirectory, listVersion, dxId));
        if (isIncidental) {
            intervalListByDXAndVersionFile = new File(
                    String.format("%1$s/resources/annotation/abeast/NCNEXUS38/Incidental/incidental_%3$s_%2$s.interval_list", dataDirectory,
                            listVersion, dxId));
        }
        if (!intervalListByDXAndVersionFile.exists()) {
            throw new WorkflowException("Interval list file does not exist: " + intervalListByDXAndVersionFile.getAbsolutePath());
        }

        File subjectDirectory = new File(subjectMergeHome, subjectName);

        List<File> files = Arrays.asList(subjectDirectory.listFiles((a, b) -> {
            if (b.endsWith(".deduped.bam")) {
                return true;
            }
            return false;
        }));

        File bamFile = null;
        if (CollectionUtils.isNotEmpty(files)) {
            bamFile = files.get(0);
        }

        if (bamFile == null) {
            logger.error("bam file to process was not found");
            throw new WorkflowException("bam file to process was not found");
        }

        files = Arrays.asList(subjectDirectory.listFiles((a, b) -> {
            if (b.endsWith(".deduped.bai")) {
                return true;
            }
            return false;
        }));

        File bamIndexFile = null;
        if (CollectionUtils.isNotEmpty(files)) {
            bamIndexFile = files.get(0);
        }

        if (bamIndexFile == null) {
            logger.error("bam index file was not found");
            throw new WorkflowException("bam index file was not found");
        }

        File outputDirectory = new File(subjectDirectory, listVersion);
        if (!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }

        try {

            // new job
            CondorJobBuilder builder = SequencingWorkflowJobFactory.createJob(++count, PicardCollectHsMetricsCLI.class, attempt.getId())
                    .siteName(siteName);
            File picardCollectHsMetricsFile = new File(outputDirectory, bamFile.getName().replace(".bam", ".hs.metrics"));
            File picardCollectHsPerTargetsCoverageFile = new File(outputDirectory, bamFile.getName().replace(".bam", ".hs.coverage"));
            builder.addArgument(PicardCollectHsMetricsCLI.INPUT, bamFile.getAbsolutePath())
                    .addArgument(PicardCollectHsMetricsCLI.OUTPUT, picardCollectHsMetricsFile.getAbsolutePath())
                    .addArgument(PicardCollectHsMetricsCLI.REFERENCESEQUENCE, referenceSequence)
                    .addArgument(PicardCollectHsMetricsCLI.PERTARGETCOVERAGE, picardCollectHsPerTargetsCoverageFile.getAbsolutePath())
                    .addArgument(PicardCollectHsMetricsCLI.BAITINTERVALS, versionedExonsIntervalListFile.getAbsolutePath())
                    .addArgument(PicardCollectHsMetricsCLI.TARGETINTERVALS, versionedExonsIntervalListFile.getAbsolutePath());
            CondorJob picardCollectHsMetricsJob = builder.build();
            logger.info(picardCollectHsMetricsJob.toString());
            graph.addVertex(picardCollectHsMetricsJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, SAMToolsDepthToGATKDOCFormatConverterCLI.class, attempt.getId())
                    .siteName(siteName).numberOfProcessors(8);
            File samtoolsDepthFile = new File(subjectDirectory, bamFile.getName().replace(".bam", ".depth.txt"));
            File samtoolsDepthConvertedFile = new File(outputDirectory,
                    bamFile.getName().replace(".bam", String.format(".depth.v%s.txt", listVersion)));
            builder.addArgument(SAMToolsDepthToGATKDOCFormatConverterCLI.INPUT, samtoolsDepthFile.getAbsolutePath())
                    .addArgument(SAMToolsDepthToGATKDOCFormatConverterCLI.OUTPUT, samtoolsDepthConvertedFile.getAbsolutePath())
                    .addArgument(SAMToolsDepthToGATKDOCFormatConverterCLI.INTERVALS, allIntervalsFile.getAbsolutePath());
            CondorJob samtoolsDepthToGATKDOCFormatConverterJob = builder.build();
            logger.info(samtoolsDepthToGATKDOCFormatConverterJob.toString());
            graph.addVertex(samtoolsDepthToGATKDOCFormatConverterJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, PicardViewSAMCLI.class, attempt.getId()).siteName(siteName);
            File picardViewSAMOutput = new File(outputDirectory,
                    bamFile.getName().replace(".bam", String.format(".filtered_by_dxid_%s_v%s.bam", dxId, listVersion)));
            builder.addArgument(PicardViewSAMCLI.OUTPUT, picardViewSAMOutput.getAbsolutePath())
                    .addArgument(PicardViewSAMCLI.INTERVALLIST, intervalListByDXAndVersionFile.getAbsolutePath())
                    .addArgument(PicardViewSAMCLI.INPUT, bamFile.getAbsolutePath());
            CondorJob samtoolsViewJob = builder.build();
            logger.info(samtoolsViewJob.toString());
            graph.addVertex(samtoolsViewJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, PicardSortSAMCLI.class, attempt.getId()).siteName(siteName);
            File picardSortSAMOutput = new File(outputDirectory, picardViewSAMOutput.getName().replace(".bam", ".sorted.bam"));
            File picardSortSAMIndexOut = new File(outputDirectory, picardSortSAMOutput.getName().replace(".bam", ".bai"));
            builder.addArgument(PicardSortSAMCLI.INPUT, picardViewSAMOutput.getAbsolutePath()).addArgument(PicardSortSAMCLI.CREATEINDEX)
                    .addArgument(PicardSortSAMCLI.OUTPUT, picardSortSAMOutput.getAbsolutePath())
                    .addArgument(PicardSortSAMCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase());
            CondorJob picardSortSAMJob = builder.build();
            logger.info(picardSortSAMJob.toString());
            graph.addVertex(picardSortSAMJob);
            graph.addEdge(samtoolsViewJob, picardSortSAMJob);

            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, ZipCLI.class, attempt.getId()).siteName(siteName);
            File zipOutputFile = new File(outputDirectory, picardSortSAMOutput.getName().replace(".bam", ".zip"));
            builder.addArgument(ZipCLI.ENTRY, picardSortSAMOutput.getAbsolutePath())
                    .addArgument(ZipCLI.WORKDIR, outputDirectory.getAbsolutePath())
                    .addArgument(ZipCLI.ENTRY, picardSortSAMIndexOut.getAbsolutePath())
                    .addArgument(ZipCLI.OUTPUT, zipOutputFile.getAbsolutePath());
            CondorJob zipJob = builder.build();
            logger.info(zipJob.toString());
            graph.addVertex(zipJob);
            graph.addEdge(picardSortSAMJob, zipJob);

            File vcf = new File(subjectDirectory, bamFile.getName().replace(".bam", ".filtered.srd.ps.va.vcf"));
            // new job
            builder = SequencingWorkflowJobFactory.createJob(++count, FilterVariantCLI.class, attempt.getId()).siteName(siteName);
            File filterVariantOutput = new File(outputDirectory,
                    bamFile.getName().replace(".bam", String.format(".filtered_by_dxid_%s_v%s.vcf", dxId, listVersion)));
            builder.addArgument(FilterVariantCLI.INTERVALLIST, intervalListByDXAndVersionFile.getAbsolutePath())
                    .addArgument(FilterVariantCLI.INPUT, vcf.getAbsolutePath())
                    .addArgument(FilterVariantCLI.OUTPUT, filterVariantOutput.getAbsolutePath());
            CondorJob filterVariantJob = builder.build();
            logger.info(filterVariantJob.toString());
            graph.addVertex(filterVariantJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt.getId()).siteName(siteName);
            builder.addArgument(RemoveCLI.FILE, picardViewSAMOutput.getAbsolutePath());
            CondorJob removeJob = builder.build();
            logger.info(removeJob.toString());
            graph.addVertex(removeJob);
            graph.addEdge(zipJob, removeJob);
            graph.addEdge(filterVariantJob, removeJob);

        } catch (Exception e) {
            throw new WorkflowException(e);
        }

        return graph;

    }

    @Override
    public void postRun() throws WorkflowException {
        logger.info("ENTERING postRun()");

        try {
            ExecutorService es = Executors.newSingleThreadExecutor();

            RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                    getWorkflowRunAttempt());
            es.submit(runnable);

            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
