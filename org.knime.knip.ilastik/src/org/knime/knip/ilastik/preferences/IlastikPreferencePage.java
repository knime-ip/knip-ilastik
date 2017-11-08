package org.knime.knip.ilastik.preferences;

import java.io.File;

import org.eclipse.jface.preference.DirectoryFieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.jface.preference.IntegerFieldEditor;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.ilastik.ilastik4ij.IlastikOptions;
import org.knime.knip.imagej2.core.IJGateway;
import org.scijava.options.OptionsService;

/**
 * Provides a preference page for setting ilastik specific preferences
 */

public class IlastikPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {
    private OptionsService optionsService = null;

    public IlastikPreferencePage() {
        super(GRID);
        setPreferenceStore(IlastikPreferencesPlugin.getDefault().getPreferenceStore());
        setDescription("Ilastik binary configuration");
    }

    /**
     * Creates the field editors. Field editors are abstractions of the common GUI blocks needed to manipulate various
     * types of preferences. Each field editor knows how to save and restore itself.
     */
    @Override
    public void createFieldEditors() {
        final String os = System.getProperty("os.name", "generic").toLowerCase();
        if (os.contains("mac") || os.contains("darwin")) {
            addField(new DirectoryFieldEditor(IlastikPreferenceConstants.P_PATH, "&Ilastik executable location:",
                    getFieldEditorParent()));
        } else {
            addField(new FileFieldEditor(IlastikPreferenceConstants.P_PATH, "&Ilastik executable location:",
                    getFieldEditorParent()));
        }
        final IntegerFieldEditor ramField = new IntegerFieldEditor(IlastikPreferenceConstants.P_RAM,
                "Max amount of &RAM (in MB) ilastik is allowed to use", getFieldEditorParent());
        ramField.setValidRange(256, Integer.MAX_VALUE);
        addField(ramField);
        IntegerFieldEditor threadsField = new IntegerFieldEditor(IlastikPreferenceConstants.P_THREADS,
                "Maximum number of threads ilastik is allowed to use (-1 for no limit)", getFieldEditorParent());
        threadsField.setValidRange(-1, Integer.MAX_VALUE);
        addField(threadsField);
    }

    @Override
    public void init(final IWorkbench workbench) {
    }

    @Override
    public boolean performOk() {
        final boolean status = super.performOk();
        if (status) {
            if (optionsService == null) {
                optionsService = IJGateway.getImageJContext().getService(OptionsService.class);
                if (optionsService == null) {
                    return false;
                }
            }

            final IlastikOptions ilastikOptions = optionsService.getOptions(IlastikOptions.class);

            if (ilastikOptions == null) {
                return false;
            }

            final IPreferenceStore preferenceStore = getPreferenceStore();
            ilastikOptions.setExecutableFilePath(preferenceStore.getString(IlastikPreferenceConstants.P_PATH));
            ilastikOptions.setMaxRamMb(preferenceStore.getInt(IlastikPreferenceConstants.P_RAM));
            ilastikOptions.setNumThreads(preferenceStore.getInt(IlastikPreferenceConstants.P_THREADS));

            if (!new File(ilastikOptions.getExecutableFilePath()).exists()) {
                this.setErrorMessage("Selected file is not a valid ilastik executable");
            }

            return ilastikOptions.isConfigured();
        }
        return status;
    }

}