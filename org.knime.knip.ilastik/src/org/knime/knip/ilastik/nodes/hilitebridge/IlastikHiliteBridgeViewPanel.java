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
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.LinkedList;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JToggleButton;
import javax.swing.table.DefaultTableModel;

import org.knime.core.data.RowKey;

/**
 * Very simple UI to demonstrate Ilastik<->KNIME Interaction
 *
 * @author Christian Dietz, University of Konstanz
 * @author Andreas Graumann, University of Konstanz
 */
public class IlastikHiliteBridgeViewPanel extends JPanel {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * RowKey of current hilite
     */
    private RowKey m_currentHilite;

    /**
     * position access
     */
    private PositionAccess m_access;

    /**
     * List with all selected hilites
     */
    private LinkedList<RowKey> m_hiliteQueue;

    /**
     * Client
     */
    private IlastikHiliteClient m_client;

    /**
     * current status of server
     */
    private JLabel m_serverStatus;

    /**
     * Table with all selected hilites
     */
    private JTable m_table = new JTable();


    /**
     * Server
     */
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
     * Initialize panel
     */
    private void init() {
        m_serverStatus = new JLabel("Server is currently not running");
        final JToggleButton serverToggle = new JToggleButton(m_server.isShutDown() ? "Start Server" : "Stop Server");

        // init action listeners
        initButtonActionListener(serverToggle);

        if (m_access == null) {
            throw new IllegalArgumentException("Please reconfigure node");
        }

        m_table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent e) {
              if (e.getClickCount() == 1) {
                JTable target = (JTable)e.getSource();
                int row = target.getSelectedRow();
                RowKey selectedObject = (RowKey) target.getModel().getValueAt(row, 0);
                m_client.firePositionChangedCommand(m_access.getPositionRowKey(m_currentHilite = selectedObject));
              }
            }
          });

        updateStatus();

        // create panel
        final JPanel serverStatusPanel = new JPanel();
        serverStatusPanel.setLayout(new BoxLayout(serverStatusPanel, BoxLayout.Y_AXIS));
        serverStatusPanel.setBorder(BorderFactory.createTitledBorder("Server Status"));
        serverStatusPanel.setSize(200, 20);

        serverStatusPanel.add(m_serverStatus);
        serverStatusPanel.add(new JLabel(" "));
        serverStatusPanel.add(serverToggle);
        add(serverStatusPanel);

        final JPanel hilitesPanel = new JPanel();
        hilitesPanel.setLayout(new BoxLayout(hilitesPanel, BoxLayout.Y_AXIS));
        hilitesPanel.setBorder(BorderFactory.createTitledBorder("Selected hilites"));
        hilitesPanel.add(new JLabel("Click on a row to center this cell in ilastik:"));

        // Add table with scrollpane
        JScrollPane pane = new JScrollPane();
        hilitesPanel.add(pane);
        pane.setViewportView(m_table);

        add(hilitesPanel);

        updateUI();
    }

    /**
     * @param serverToggle
     */
    private void initButtonActionListener(final JToggleButton serverToggle) {
        serverToggle.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                if (m_server.isShutDown()) {
                    serverToggle.setText("Stop Server");
                    m_serverStatus.setText("Server is running on port " + m_server.getPort());
                    m_server.startUp();
                    updateUI();
                } else {
                    serverToggle.setText("Start Server");
                    m_serverStatus.setText("Server currently not running");
                    m_server.shutDown();
                    updateUI();
                }
            }
        });
    }

    /**
     *
     */
    private void updateStatus() {
        // create table data
        String[] header = {"Cell"};
        RowKey[][] data = new RowKey[m_hiliteQueue.size()][2];

        // fill data
        for (int i = 0; i < m_hiliteQueue.size(); i++) {
            data[i][0] = m_hiliteQueue.get(i);
        }

        // create table model
        DefaultTableModel tableModel = new DefaultTableModel(data, header);
        m_table.setModel(tableModel);

        updateUI();
    }
}
