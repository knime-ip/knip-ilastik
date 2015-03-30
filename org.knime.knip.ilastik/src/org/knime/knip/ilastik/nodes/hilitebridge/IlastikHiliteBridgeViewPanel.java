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
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
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

    private MapPositionAccess m_mapAccess;



    /**
     *
     * Constructor
     *
     * @param positionAccess
     * @param client
     * @param server
     * @param mapAccess
     */
    public IlastikHiliteBridgeViewPanel(final PositionAccess positionAccess, final IlastikHiliteClient client,
                                        final IlastikHiliteServer server, final MapPositionAccess mapAccess) {
        m_access = positionAccess;
        m_client = client;
        m_server = server;
        m_hiliteQueue = new LinkedList<RowKey>();
        m_mapAccess = mapAccess;

        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
        init();
    }

    /**
     * Clears all hilites
     */
    public void clearAllHilites() {
        m_hiliteQueue.clear();
        m_currentHilite = null;
        updateStatus();
    }

    /**
     * Clear a set of hilites
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
     * Update current hilites
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
        final JButton serverStart = new JButton("Start Server");
        final JButton serverStop = new JButton("Stop Server");

        // init action listeners
        startServer(serverStart);
        stopServer(serverStop);

        if (m_access == null) {
            throw new IllegalArgumentException("Please reconfigure node");
        }

        // mouse listener, the user can click on a row within the table to move the ilastik viewer to this position
        m_table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent e) {
              if (e.getClickCount() == 1) {
                JTable target = (JTable)e.getSource();
                int row = target.getSelectedRow();
                String selectedObject = (String) target.getModel().getValueAt(row, 0);
                RowKey key = new RowKey(selectedObject);
                m_client.sendPositionChangedCommand(m_access.getPositionRowKey(m_currentHilite = key));
              }
            }
          });

        updateStatus();

        // create panel
        final JPanel serverStatusPanel = new JPanel();
        serverStatusPanel.setLayout(new BoxLayout(serverStatusPanel, BoxLayout.X_AXIS));
        serverStatusPanel.setBorder(BorderFactory.createTitledBorder("Server Status"));
        serverStatusPanel.setSize(200, 20);

        serverStatusPanel.add(serverStart);
        serverStatusPanel.add(serverStop);
        serverStatusPanel.add(m_serverStatus);
        serverStatusPanel.add(new JLabel(" "));

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
     * Action Listener for start button
     *
     * @param btn
     */
    private void startServer(final JButton btn) {
        btn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                //if (m_server.isShutDown()) {
                    m_server.startUp();

                    if (!m_server.isShutDown()) {
                        m_serverStatus.setText("Server is running on port " + m_server.getPort());
                        updateUI();
                    }

                    if(!m_client.isConnected()) {
                        m_serverStatus.setText("Client is not connected. Please start Ilastik Server.");
                        updateUI();
                    }

               // }
            }
        });
    }

    /**
     * Action Listener for stop button
     *
     * @param btn
     */
    private void stopServer(final JButton btn) {
        btn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                if (!m_server.isShutDown()) {
                    m_server.shutDown();
                    if (m_server.isShutDown()) {
                        m_serverStatus.setText("Server currently not running");
                        updateUI();
                    }
                }
            }
        });
    }

    /**
     * Update Current Status and table with current hilites
     */
    private void updateStatus() {
        // create table data
        String[] header = {"RowId", "Ilastik ID", "X", "Y", "Z", "Channel", "Time"};
        String[][] data = new String[m_hiliteQueue.size()][7];

        // fill data
        for (int i = 0; i < m_hiliteQueue.size(); i++) {
            double[] pos = m_mapAccess.getPositionRowKey(new RowKey(m_hiliteQueue.get(i).getString()));
            data[i][0] = m_hiliteQueue.get(i).getString();
            data[i][1] = String.valueOf(Math.round(pos[5]));
            data[i][2] = String.valueOf(Math.round(pos[0]));
            data[i][3] = String.valueOf(Math.round(pos[1]));
            data[i][4] = String.valueOf(Math.round(pos[2]));
            data[i][5] = String.valueOf(Math.round(pos[3]));
            data[i][6] = String.valueOf(Math.round(pos[4]));
        }

        // create table model
        DefaultTableModel tableModel = new DefaultTableModel(data, header);
        m_table.setModel(tableModel);

        updateUI();
    }
}
