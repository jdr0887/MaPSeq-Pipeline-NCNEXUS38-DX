package edu.unc.mapseq.commons.ncnexus38.dx;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Job;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.Zip;
import edu.unc.mapseq.module.sequencing.filter.FilterVariant;
import edu.unc.mapseq.module.sequencing.gatk.GATKDepthOfCoverage;
import edu.unc.mapseq.module.sequencing.picard.PicardSortSAM;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndex;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsView;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    private WorkflowRunAttempt workflowRunAttempt;

    public RegisterToIRODSRunnable() {
        super();
    }

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService mapseqDAOBeanService, WorkflowRunAttempt workflowRunAttempt) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
        this.workflowRunAttempt = workflowRunAttempt;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        try {

            WorkflowRun workflowRun = workflowRunAttempt.getWorkflowRun();
            Workflow workflow = workflowRun.getWorkflow();

            Set<Sample> sampleSet = SequencingWorkflowUtil.getAggregatedSamples(mapseqDAOBeanService, workflowRunAttempt);

            if (CollectionUtils.isEmpty(sampleSet)) {
                logger.warn("No Samples found");
                return;
            }

            String version = null;
            String dx = null;

            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (CollectionUtils.isNotEmpty(attributeSet)) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
                    if ("GATKDepthOfCoverage.interval_list.version".equals(attribute.getName())) {
                        version = attribute.getValue();
                    }
                    if ("SAMToolsView.dx.id".equals(attribute.getName())) {
                        dx = attribute.getValue();
                    }
                }
            }

            if (version == null && dx == null) {
                logger.warn("Both version and dx were null");
                return;
            }

            logger.info("version = {}", version);
            logger.info("dx = {}", dx);

            for (Sample sample : sampleSet) {

                // assumption: a dash is used as a delimiter between a participantId and
                // the external code
                int idx = sample.getName().lastIndexOf("-");
                String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();
                logger.info("participantId = {}", participantId);

                File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);
                File tmpDir = new File(outputDirectory, "tmp");
                if (!tmpDir.exists()) {
                    tmpDir.mkdirs();
                }

                String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncnexus/analysis/%s/L%03d_%s/%s/%s", workflow.getSystem().getValue(),
                        sample.getFlowcell().getName(), sample.getLaneIndex(), sample.getBarcode(), workflow.getName(), version);

                List<CommandInput> commandInputList = new LinkedList<CommandInput>();

                CommandOutput commandOutput = null;

                CommandInput commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s Project NCNEXUS%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s ParticipantId %s NCNEXUS%n", irodsDirectory, participantId));
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

                String rootFileName = String.format("%s_%s_L%03d.fixed-rg.deduped.realign.fixmate.recal", sample.getFlowcell().getName(),
                        sample.getBarcode(), sample.getLaneIndex());

                List<ImmutablePair<String, String>> attributeList = Arrays.asList(new ImmutablePair<String, String>("ParticipantId", participantId),
                        new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                        new ImmutablePair<String, String>("MaPSeqWorkflowName", workflow.getName()),
                        new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                        new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                        new ImmutablePair<String, String>("MaPSeqSystem", workflow.getSystem().getValue()),
                        new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()),
                        new ImmutablePair<String, String>("DxID", dx), new ImmutablePair<String, String>("DxVersion", version));

                List<ImmutablePair<String, String>> attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                File file = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_cumulative_coverage_counts", rootFileName, version));
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                file = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_cumulative_coverage_proportions", rootFileName, version));
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                file = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_interval_statistics", rootFileName, version));
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                file = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_interval_summary", rootFileName, version));
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                file = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_statistics", rootFileName, version));
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKDepthOfCoverage.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
                file = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_summary", rootFileName, version));
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", SAMToolsView.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM.toString()));
                file = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.bam", rootFileName, dx, version));
                Job job = SequencingWorkflowUtil.findJob(mapseqDAOBeanService, workflowRunAttempt.getId(), SAMToolsView.class.getName(), file);
                if (job != null) {
                    attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobId", job.getId().toString()));
                } else {
                    logger.warn(String.format("Couldn't find job for: %d, %s", workflowRunAttempt.getId(), SAMToolsView.class.getName()));
                }
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", PicardSortSAM.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM.toString()));
                file = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.sorted.bam", rootFileName, dx, version));
                job = SequencingWorkflowUtil.findJob(mapseqDAOBeanService, workflowRunAttempt.getId(), PicardSortSAM.class.getName(), file);
                if (job != null) {
                    attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobId", job.getId().toString()));
                } else {
                    logger.warn(String.format("Couldn't find job for: %d, %s", workflowRunAttempt.getId(), PicardSortSAM.class.getName()));
                }
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", SAMToolsIndex.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM_INDEX.toString()));
                file = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.sorted.bai", rootFileName, dx, version));
                job = SequencingWorkflowUtil.findJob(mapseqDAOBeanService, workflowRunAttempt.getId(), SAMToolsIndex.class.getName(), file);
                if (job != null) {
                    attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobId", job.getId().toString()));
                } else {
                    logger.warn(String.format("Couldn't find job for: %d, %s", workflowRunAttempt.getId(), SAMToolsIndex.class.getName()));
                }
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", Zip.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_ZIP.toString()));
                file = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.sorted.zip", rootFileName, dx, version));
                job = SequencingWorkflowUtil.findJob(mapseqDAOBeanService, workflowRunAttempt.getId(), Zip.class.getName(), file);
                if (job != null) {
                    attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobId", job.getId().toString()));
                } else {
                    logger.warn(String.format("Couldn't find job for: %d, %s", workflowRunAttempt.getId(), Zip.class.getName()));
                }
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FilterVariant.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
                file = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.vcf", rootFileName, dx, version));
                job = SequencingWorkflowUtil.findJob(mapseqDAOBeanService, workflowRunAttempt.getId(), FilterVariant.class.getName(), file);
                if (job != null) {
                    attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobId", job.getId().toString()));
                } else {
                    logger.warn(String.format("Couldn't find job for: %d, %s", workflowRunAttempt.getId(), FilterVariant.class.getName()));
                }
                files2RegisterToIRODS.add(new IRODSBean(file, attributeListWithJob));

                for (IRODSBean bean : files2RegisterToIRODS) {

                    File f = bean.getFile();
                    if (!f.exists()) {
                        logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                        continue;
                    }

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);

                    StringBuilder registerCommandSB = new StringBuilder();
                    String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", f.getAbsolutePath(), irodsDirectory, f.getName());
                    String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, f.getName());
                    registerCommandSB.append(registrationCommand).append("\n");
                    registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                    commandInput.setCommand(registerCommandSB.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);
                    sb = new StringBuilder();
                    for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                        sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCNEXUSDX%n", irodsDirectory, bean.getFile().getName(),
                                attribute.getLeft(), attribute.getRight()));
                    }
                    commandInput.setCommand(sb.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                }

                File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                Executor executor = BashExecutor.getInstance();

                for (CommandInput ci : commandInputList) {
                    try {
                        commandOutput = executor.execute(ci, mapseqrc);
                        if (commandOutput.getExitCode() != 0) {
                            logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                            logger.warn("command failed: {}", ci.getCommand());
                        }
                        logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                    } catch (ExecutorException e) {
                        if (commandOutput != null) {
                            logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                        }
                    }
                }

            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public MaPSeqDAOBeanService getMapseqDAOBeanService() {
        return mapseqDAOBeanService;
    }

    public void setMapseqDAOBeanService(MaPSeqDAOBeanService mapseqDAOBeanService) {
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

    public WorkflowRunAttempt getWorkflowRunAttempt() {
        return workflowRunAttempt;
    }

    public void setWorkflowRunAttempt(WorkflowRunAttempt workflowRunAttempt) {
        this.workflowRunAttempt = workflowRunAttempt;
    }

}
