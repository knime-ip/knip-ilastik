package org.knime.knip.ilastik.preferences;

import org.eclipse.ui.plugin.AbstractUIPlugin;

/**
 * Plugin instance for the Ilastik integration
 */
public class IlastikPreferencesPlugin extends AbstractUIPlugin {

    private static IlastikPreferencesPlugin instance = new IlastikPreferencesPlugin();

    public static IlastikPreferencesPlugin getDefault() {
        return instance;
    }
}
