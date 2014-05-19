/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2014
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
 * Created on 15.05.2014 by Christian Dietz
 */
package org.knime.knip.ilastik.nodes.hilitebridge;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.LinkedList;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JToggleButton;

import org.knime.core.data.RowKey;

/**
 * Very simple UI to demonstrate Ilastik<->KNIME Interaction
 *
 * @author Christian Dietz
 */
public class IlastikHiliteBridgeViewPanel extends JPanel {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private RowKey m_currentHilite;

    private PositionAccess m_access;

    private LinkedList<RowKey> m_hiliteQueue;

    private IlastikHiliteClient m_client;

    private JLabel m_hilitesLeftLabel;

    private JButton m_nextButton;

    private JLabel m_currentHiliteLabel;

    private IlastikHiliteServer m_server;

    /**
     * @param positionAccess
     * @param client
     * @param server
     */
    public IlastikHiliteBridgeViewPanel(final PositionAccess positionAccess, final IlastikHiliteClient client,
                                        final IlastikHiliteServer server) {
        m_access = positionAccess;
        m_client = client;
        m_server = server;
        m_hiliteQueue = new LinkedList<RowKey>();
        init();
    }

    /**
     *
     */
    public void clearAllHilites() {
        m_hiliteQueue.clear();
        m_currentHilite = null;
        updateStatus();
    }

    /**
     *
     * @param hilites
     */
    public void clearHilites(final Set<RowKey> hilites) {
        m_hiliteQueue.removeAll(hilites);
        if (hilites.contains(m_currentHilite)) {
            m_currentHilite = null;
        }
        updateStatus();
    }

    /**
     *
     * @param hilites
     */
    public void updateHilites(final Set<RowKey> hilites) {
        m_hiliteQueue.addAll(hilites);
        updateStatus();
    }

    /**
     *
     */
    private void init() {
        m_hilitesLeftLabel = new JLabel();
        m_currentHiliteLabel = new JLabel();
        m_nextButton = new JButton();

        final JToggleButton serverToggle = new JToggleButton(m_server.isShutDown() ? "Start Server" : "Stop Server");

        serverToggle.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                if (m_server.isShutDown()) {
                    serverToggle.setText("Stop Server");
                    m_server.startUp();
                    updateUI();
                } else {
                    serverToggle.setText("Start Server");
                    m_server.shutDown();
                    updateUI();
                }
            }
        });

        m_nextButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                if (m_hiliteQueue.isEmpty()) {
                    // do nothing...;-)
                } else {
                    m_client.firePositionChangedCommand(m_access.getPositionRowKey(m_currentHilite =
                            m_hiliteQueue.pop()));
                    updateStatus();
                }
            }
        });

        updateStatus();

        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.add(m_hilitesLeftLabel);
        panel.add(m_currentHiliteLabel);
        panel.add(m_nextButton);
        panel.add(new JLabel("------------------------"));
        panel.add(serverToggle);
        add(panel);
        updateUI();
    }

    /**
     * @return
     */
    private void updateStatus() {
        m_hilitesLeftLabel.setText("Number Hilites: [" + m_hiliteQueue.size() + "]");

        m_currentHiliteLabel.setText("Current Hilite: "
                + (m_currentHilite == null ? "[Nothing Selected] " : "[" + m_currentHilite.getString() + "]"));

        if (m_hiliteQueue.size() > 0) {
            m_nextButton.setText("Next: [" + m_hiliteQueue.get(0).getString() + "]");
        } else {
            m_nextButton.setText("Next: [Nothing to Hilite]");
        }
        updateUI();
    }
}
