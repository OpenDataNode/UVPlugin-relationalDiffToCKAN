package eu.unifiedviews.plugins.loader.relationaldifftockan;

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;

public class RelationalDiffToCkanVaadinDialog extends AbstractDialog<RelationalDiffToCkanConfig_V1> {

    private static final long serialVersionUID = 3627915032084995009L;

    public RelationalDiffToCkanVaadinDialog() {
        super(RelationalDiffToCkan.class);
    }

    @Override
    protected void buildDialogLayout() {
        // DPU has no dialog
    }

    @Override
    protected void setConfiguration(RelationalDiffToCkanConfig_V1 conf) throws DPUConfigException {
        // DPU has no configuration
    }

    @Override
    protected RelationalDiffToCkanConfig_V1 getConfiguration() throws DPUConfigException {
        RelationalDiffToCkanConfig_V1 config = new RelationalDiffToCkanConfig_V1();
        return config;
    }

}
