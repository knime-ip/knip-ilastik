/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2015
 *  University of Konstanz, Germany and
 *  KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * Created on Sep 28, 2015 by andreasgraumann
 */
package org.knime.knip.ilastik.nodes;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.knime.core.node.NodeLogger;
import org.osgi.service.prefs.BackingStoreException;

/**
 *
 * Preference page for configurations. Path of ilastik installation.
 *
 * @author Andreas Graumann, University of Konstanz
 */
public class IlastikPreferencePage extends PreferencePage
        implements IWorkbenchPreferencePage {

    /**
     *
     */
    private static final String PLUGIN_PATH = "org.knime.knip.ilastik.nodes";

    private static final String DEFAULT_PATH = doAutoGuessCellProfilerPath();

    private ScrolledComposite m_sc;

    private Composite m_container;

    private FileFieldEditor m_fileEditor;

    private static final NodeLogger LOGGER =
            NodeLogger.getLogger(IlastikPreferencePage.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final IWorkbench workbench) {
        // nothing to do here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Control createContents(final Composite parent) {
        m_sc = new ScrolledComposite(parent, SWT.H_SCROLL | SWT.V_SCROLL);
        m_container = new Composite(m_sc, SWT.NONE);
        m_container.setLayout(new GridLayout());
        m_fileEditor = new FileFieldEditor(PLUGIN_PATH,
                "Path to Ilastik Installation", m_container);
        m_fileEditor.setStringValue(Platform.getPreferencesService().getString(
                PLUGIN_PATH, "path", DEFAULT_PATH, null));
        GridData gridData = new GridData();
        gridData.horizontalSpan = 3;
        gridData = new GridData();
        gridData.horizontalSpan = 3;
        gridData.verticalIndent = 20;
        m_sc.setContent(m_container);
        m_sc.setExpandHorizontal(true);
        m_sc.setExpandVertical(true);
        return m_sc;
    }

    private static String getOS() {
        return System.getProperty("os.name", "generic").toLowerCase();
    }

    private static String doAutoGuessCellProfilerPath() {
        final String os = getOS();
        if ((os.indexOf("mac") >= 0) || (os.indexOf("darwin") >= 0)) {
            return "/Applications/ilastik-1.1.7-OSX.app";
        } else if (os.indexOf("win") >= 0) {
            return "C:\\Program Files\\ilastik-1.1.7-win64.exe";
        } else if (os.indexOf("nux") >= 0) {
            return "/usr/bin/ilastik-1.1.7-Linux/runIlastik.sh";
        } else {
            return "";
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean performOk() {
        performApply();
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void performApply() {
        setPath(m_fileEditor.getStringValue());
    }

    /**
     * Saves the given path.
     *
     * @param path
     *            Path to the CellProfiler module
     */
    private void setPath(final String path) {
        IEclipsePreferences prefs =
                InstanceScope.INSTANCE.getNode(PLUGIN_PATH);
        prefs.put("path", path);
        try {
            prefs.flush();
        } catch (BackingStoreException e) {
            LOGGER.error("Could not save preferences: " + e.getMessage(), e);
        }
    }

    /**
     *
     * @return Path of Ilastik installation
     */
    public static String getPath() {

        final String path = Platform.getPreferencesService().getString(
                PLUGIN_PATH, "path", DEFAULT_PATH, null);

        final String os = getOS();
        String macExtension = "";
        // On Mac OS X we must call the program within the app to be able to add
        // arguments
        if ((os.indexOf("mac") >= 0) || (os.indexOf("darwin") >= 0)) {
            macExtension = "/Contents/MacOS/ilastik";
        }
        return path.concat(macExtension);
    }

}
