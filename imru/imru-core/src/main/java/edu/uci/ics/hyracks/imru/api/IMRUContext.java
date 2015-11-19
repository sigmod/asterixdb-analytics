package org.apache.hyracks.imru.api;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;

/**
 * @author Rui Wang
 */
public class IMRUContext {
    private String operatorName;
    private NodeControllerService nodeController;

    private String nodeId;
    protected IHyracksTaskContext ctx;

    public IMRUContext(IHyracksTaskContext ctx) {
        this(ctx, null);
    }

    public IMRUContext(IHyracksTaskContext ctx, String operatorName) {
        this.ctx = ctx;
        this.operatorName = operatorName;
        IHyracksJobletContext jobletContext = ctx.getJobletContext();
        if (jobletContext instanceof Joblet) {
            this.nodeController = ((Joblet) jobletContext).getNodeController();
            this.nodeId = nodeController.getId();
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public ByteBuffer allocateFrame() throws HyracksDataException {
        return ctx.allocateFrame();
    }

    public int getFrameSize() {
        return ctx.getInitialFrameSize();
    }

    public IHyracksJobletContext getJobletContext() {
        return ctx.getJobletContext();
    }

    public IMRURuntimeContext getRuntimeContext() {
        INCApplicationContext appContext = getJobletContext().getApplicationContext();
        return (IMRURuntimeContext) appContext.getApplicationObject();
    }

    /**
     * Get the model shared in each node controller
     *
     * @return
     */
    public Serializable getModel() {
        INCApplicationContext appContext = getJobletContext().getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
        return context.model;
    }

    /**
     * Set the model shared in each node controller
     */
    public void setModel(Serializable model) {
        INCApplicationContext appContext = getJobletContext().getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
        context.model = model;
    }

    /**
     * Set the model shared in each node controller
     */
    public void setModel(Serializable model, int age) {
        INCApplicationContext appContext = getJobletContext().getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
        context.model = model;
        context.modelAge = age;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }
}
