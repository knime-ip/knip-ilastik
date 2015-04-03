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
 * Created on 14.05.2014 by Christian Dietz
 */
package org.knime.knip.ilastik.nodes.hilitebridge;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;

import org.knime.core.data.RowKey;

/**
 *
 * @author Andreas Graumann, University of Konstanz
 * @author Christian Dietz, University of Konstanz
 */
public class IlastikHiliteServer {

    enum HiliteEvents {
        hilite, unhilite, clearhilite, toggle;
    }

    /**
     * IDENTIFIER OF COMMANDS
     */
    public final static String COMMAND_NAME = "command";

    /**
     * IDENDTIFIER FOR ROWIDs
     */
    public final static String OID_NAME = "objectid";

    /**
     * Identifier for ilastik id
     */
    public final static String ILASTIK_ID = "ilastik_id";

    /**
     * Identifier for time id
     */
    public final static String TIME_ID = "time_id";

    /**
     * Status of server
     */
    private boolean m_isShutDown = true;

    /**
     * Server socket
     */
    private ServerSocket m_server;

    /**
     * All hilite listeners
     */
    private List<IlastikHiliteListenerServer> m_listeners;

    /**
     * All clients
     */
    private List<Socket> m_clients = new ArrayList<Socket>();

    /**
     * Port
     */
    private int m_port;

    /**
     * Ilastik hilite client
     */
    private IlastikHiliteClient m_client;

    /**
     * Node Model
     */
    private IlastikHiliteBridgeNodeModel m_nodeModel;

    /**
     * Constructor
     *
     * @param portNumber
     * @param client
     * @param model
     */
    IlastikHiliteServer(final int portNumber, final IlastikHiliteClient client, final IlastikHiliteBridgeNodeModel model) {
        m_listeners = new ArrayList<IlastikHiliteListenerServer>();
        m_port = portNumber;
        m_client = client;
        m_nodeModel = model;
    }

    /**
     * Start server!
     */
    void startUp() {
        if (!m_isShutDown) {

            if (!m_client.isConnected()) {
                m_client.sendHandshakeToIlastik(m_port);
            }

            return;
        }

        m_isShutDown = false;

        try {
            m_server = new ServerSocket(m_port);

            new Thread(new Runnable() {

                @Override
                public void run() {
                    System.out.println("starting server...");
                    m_client.sendHandshakeToIlastik(m_port);
                    while (!m_isShutDown) {
                        try {
                            final Socket client = m_server.accept();
                            m_clients.add(client);

                            new Thread(new Runnable() {

                                @Override
                                public void run() {
                                    while (client.isConnected()) {
                                        try {

                                            JsonObject obj = Json.createReader(client.getInputStream()).readObject();
                                            // get mode
                                            String mode = obj.get("mode").toString();
                                            if (mode.equals("\"clear\"")) {
                                                fireClearHiliteEvent();
                                                System.out.println("clear");
                                                break;
                                            }


                                            // get positions to hilite
                                            JsonObject objwehre = obj.getJsonObject("where");

                                            JsonArray arr = objwehre.getJsonArray("operands");

                                            int time = -1;
                                            int ilastik_id = -1;

                                            for (int i = 0; i < arr.size(); i++) {
                                                JsonObject o = arr.getJsonObject(i);
                                                String s = o.getString("row");
                                                int val = o.getInt("value");
                                                if (s.equals("time")) {
                                                    time = val;
                                                } else if (s.equals("ilastik_id")) {
                                                    ilastik_id = val;
                                                }
                                            }

                                            // resolve row key
                                            RowKey key = m_nodeModel.resolveRowIds(ilastik_id, time);

                                            if (mode.equals("\"hilite\"")) {
                                                fireHiLiteEvent(key.toString());
                                            } else if (mode.equals("\"unhilite\"")) {
                                                fireUnHiliteEvent(key.toString());
                                            } else if (mode.equals("\"toggle\"")) {
                                                fireToggleEvent(key.toString());
                                            } else if (mode.equals("\"clear\"")) {
                                                fireClearHiliteEvent();
                                            } else {
                                                System.err.println("Ilastik Hilite Server: Unknown Mode command!");
                                            }
                                            break;
                                        } catch (Exception e) {
                                            System.out
                                                    .println("Ilastik Hilite Server: Can not execute hilite command!");
                                            m_clients.remove(client);
                                            break;
                                        }
                                    }
                                }
                            }).start();

                        } catch (final SocketException e) {
                            System.out.println("shutting down server while listening to something ...");
                        } catch (final Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Shuts down the server
     */
    public void shutDown() {
        m_isShutDown = true;
        try {
            m_server.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Listeners
    private void fireHiLiteEvent(final String key) {
        for (final IlastikHiliteListenerServer listener : m_listeners) {
            listener.keyHilited(key);
        }
    }

    // Listeners
    private void fireUnHiliteEvent(final String key) {
        for (final IlastikHiliteListenerServer listener : m_listeners) {
            listener.keyUnhilited(key);
        }
    }

    // Listeners
    private void fireToggleEvent(final String key) {
        for (final IlastikHiliteListenerServer listener : m_listeners) {
            listener.toggleHilite(key);
        }
    }

    // Listeners
    private void fireClearHiliteEvent() {
        for (final IlastikHiliteListenerServer listener : m_listeners) {
            listener.clearHilites();
        }
    }

    /**
     * Register a listener
     *
     * @param listener
     */
    public void unregisterListener(final IlastikHiliteListenerServer listener) {
        m_listeners.remove(listener);
    }

    /**
     * Unregister a listener
     *
     * @param listener
     */
    public void registerListener(final IlastikHiliteListenerServer listener) {
        m_listeners.add(listener);
    }

    /**
     * @return serverStatus
     */
    public boolean isShutDown() {
        for (final Socket client : m_clients) {
            try {
                client.close();
            } catch (IOException e) {
                System.out.println("error during closing clients");
            }
        }
        m_clients.clear();
        return m_isShutDown;
    }

    /**
     *
     * @return port
     */
    public int getPort() {
        return m_port;
    }
}
