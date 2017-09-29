package org.knime.knip.ilastik.preferences;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.jface.preference.IPreferenceStore;

/**
 * Class used to initialize default preference values.
 */
public class PreferenceInitializer extends AbstractPreferenceInitializer {

        @Override
        public void initializeDefaultPreferences() {
                IPreferenceStore store = IlastikPreferencesPlugin.getDefault().getPreferenceStore();
                store.setDefault(IlastikPreferenceConstants.P_THREADS, -1);
                store.setDefault(IlastikPreferenceConstants.P_RAM, 512);
                store.setDefault(IlastikPreferenceConstants.P_PATH, "");
        }
}
