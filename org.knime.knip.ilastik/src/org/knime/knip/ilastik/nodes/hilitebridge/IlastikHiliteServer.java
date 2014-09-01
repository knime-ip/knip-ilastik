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
import javax.json.JsonObject;

/**
 *
 * @author Christian Dietz, University of Konstanz
 */
public class IlastikHiliteServer {

    enum HiliteEvents {
        HILITE, UNHILITE, CLEARHILITE;
    }

    /**
     * IDENTIFIER OF COMMANDS
     */
    public final static String COMMAND_NAME = "command";

    /**
     * IDENDTIFIER FOR ROWIDs
     */
    public final static String ID_NAME = "objectid";

    private boolean m_isShutDown = true;

    private ServerSocket m_server;

    private List<IlastikHiliteListenerServer> m_listeners;

    private List<Socket> m_clients = new ArrayList<Socket>();

    private int m_port;

    private IlastikHiliteClient m_client;

    IlastikHiliteServer(final int portNumber, final IlastikHiliteClient client) {
        m_listeners = new ArrayList<IlastikHiliteListenerServer>();
        m_port = portNumber;
        m_client = client;
    }

    /**
     *
     */
    void startUp() {
        if (!m_isShutDown) {
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

                            //TODO Only accept from localhost
                            //                            if(client.getLocalAddress().equals(m_server.getInetAddress().getLocalHost())){
                            //
                            //                            }

                            new Thread(new Runnable() {

                                @Override
                                public void run() {
                                    while (client.isConnected()) {
                                        try {
                                            JsonObject obj = Json.createReader(client.getInputStream()).readObject();

                                            switch (HiliteEvents.values()[obj.getInt(COMMAND_NAME)]) {
                                                case HILITE:
                                                    fireHiLiteEvent(obj.getString(ID_NAME));
                                                    break;
                                                case CLEARHILITE:
                                                    fireClearHiliteEvent();
                                                    break;
                                                case UNHILITE:
                                                    fireUnHiliteEvent(obj.getString(ID_NAME));
                                                    break;
                                            }
                                        } catch (Exception e) {
                                            System.out.println("connection with client interrupted!");
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
     * @return
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
     * @return
     */
    public int getPort() {
        return m_port;
    }
}
