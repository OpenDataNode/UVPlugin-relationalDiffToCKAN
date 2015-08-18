package org.opendatanode.plugins.loader.relationaldifftockan;

import com.vaadin.data.Validator;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;
import eu.unifiedviews.plugins.loader.relationaldifftockan.RelationalDiffToCkanConfig_V1;

public class RelationalDiffToCkanVaadinDialog extends AbstractDialog<RelationalDiffToCkanConfig_V1> {

    private static final long serialVersionUID = 3627915032084995009L;

    private VerticalLayout mainLayout;

    private TextField txtResourceName;

    public RelationalDiffToCkanVaadinDialog() {
        super(RelationalDiffToCkan.class);
    }

    @Override
    protected void buildDialogLayout() {
        setWidth("100%");
        setHeight("100%");

        this.mainLayout = new VerticalLayout();
        this.mainLayout.setWidth("100%");
        this.mainLayout.setHeight("-1px");
        this.mainLayout.setSpacing(true);
        this.mainLayout.setMargin(true);

        this.txtResourceName = new TextField();
        this.txtResourceName.setNullRepresentation("");
        this.txtResourceName.setCaption(this.ctx.tr("dialog.ckan.resource.name"));
        this.txtResourceName.setWidth("100%");
        this.txtResourceName.setDescription(this.ctx.tr("dialog.resource.name.help"));
        this.txtResourceName.addValidator(createResourceNameValidator());
        this.txtResourceName.setInputPrompt(this.ctx.tr("dialog.resource.name.input"));
        this.mainLayout.addComponent(this.txtResourceName);

        Panel panel = new Panel();
        panel.setSizeFull();
        panel.setContent(this.mainLayout);
        setCompositionRoot(panel);
    }

    private Validator createResourceNameValidator() {
        Validator validator = new Validator() {

            private static final long serialVersionUID = -186376062628005948L;

            @SuppressWarnings("unqualified-field-access")
            @Override
            public void validate(Object value) throws InvalidValueException {
                if (value == null || (value.getClass() == String.class && !((String) value).isEmpty())) {
                    return;
                }
                throw new Validator.InvalidValueException(ctx.tr("dialog.errors.params"));
            }
        };

        return validator;
    }

    @Override
    protected void setConfiguration(RelationalDiffToCkanConfig_V1 config) throws DPUConfigException {
        this.txtResourceName.setValue(config.getResourceName());
    }

    @Override
    protected RelationalDiffToCkanConfig_V1 getConfiguration() throws DPUConfigException {
        RelationalDiffToCkanConfig_V1 config = new RelationalDiffToCkanConfig_V1();
        config.setResourceName(this.txtResourceName.getValue());
        return config;
    }

}
