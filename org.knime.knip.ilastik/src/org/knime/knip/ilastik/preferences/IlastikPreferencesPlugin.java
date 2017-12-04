package org.knime.knip.ilastik.preferences;

import java.io.File;

import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.ilastik.ilastik4ij.IlastikOptions;
import org.knime.knip.imagej2.core.IJGateway;
import org.scijava.options.OptionsService;

/**
 * Plugin instance for the Ilastik integration
 */
public class IlastikPreferencesPlugin extends AbstractUIPlugin {

    private static IlastikPreferencesPlugin instance = new IlastikPreferencesPlugin();

    public static IlastikPreferencesPlugin getDefault() {
        return instance;
    }

    // init the ilastik options
    void initOptionsService() {
        OptionsService optionsService = IJGateway.getImageJContext().getService(OptionsService.class);
        if (optionsService == null) {
            throw new IllegalStateException("Could not load options service");
        }

        final IlastikOptions ilastikOptions = optionsService.getOptions(IlastikOptions.class);
        if (ilastikOptions == null) {
            throw new IllegalStateException("Could not load IlastikOptions service");
        }

        final IPreferenceStore preferenceStore = getPreferenceStore();
        ilastikOptions.setExecutableFilePath(preferenceStore.getString(IlastikPreferenceConstants.P_PATH));
        ilastikOptions.setMaxRamMb(preferenceStore.getInt(IlastikPreferenceConstants.P_RAM));
        ilastikOptions.setNumThreads(preferenceStore.getInt(IlastikPreferenceConstants.P_THREADS));

        if (!new File(ilastikOptions.getExecutableFilePath()).exists()) {
            throw new IllegalStateException("Selected file is not a valid ilastik executable");
        }
    }
}
