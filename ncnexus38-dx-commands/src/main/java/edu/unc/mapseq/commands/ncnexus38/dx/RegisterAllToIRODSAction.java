package edu.unc.mapseq.commands.ncnexus38.dx;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncnexus38.dx.RegisterToIRODSRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;

@Command(scope = "ncnexus38-dx", name = "register-all-to-irods", description = "Register all WorkflowRunAttempts to iRODS")
@Service
public class RegisterAllToIRODSAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(RegisterAllToIRODSAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");
        try {
            ExecutorService es = Executors.newFixedThreadPool(2);
            List<Workflow> foundWorkflows = maPSeqDAOBeanService.getWorkflowDAO().findByName("NCNEXUSDX");

            if (CollectionUtils.isEmpty(foundWorkflows)) {
                logger.warn("No workflows found...looking for NCNEXUSDX");
                return null;
            }

            for (Workflow workflow : foundWorkflows) {
                List<WorkflowRunAttempt> workflowRunAttemptList = maPSeqDAOBeanService.getWorkflowRunAttemptDAO()
                        .findByWorkflowId(workflow.getId());

                if (CollectionUtils.isEmpty(workflowRunAttemptList)) {
                    logger.warn("No WorkflowRunAttempts found for NCNEXUSDX");
                    return null;
                }

                logger.info("workflowRunAttemptList.size() = {}", workflowRunAttemptList.size());
                for (WorkflowRunAttempt workflowRunAttempt : workflowRunAttemptList) {
                    if (!workflowRunAttempt.getStatus().equals(WorkflowRunAttemptStatusType.DONE)) {
                        continue;
                    }
                    es.submit(new RegisterToIRODSRunnable(maPSeqDAOBeanService, workflowRunAttempt));
                }
            }
            es.shutdown();
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
