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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;

/**
 * Simple implementation of ilastik hilite client
 *
 *
 * @author Christian Dietz, University of Konstanz
 * @author Andreas Graumann, University of Konstanz
 */
public class DefaultIlastikHiliteClient implements IlastikHiliteClient {

    /**
     * Store socket
     */
    private Socket m_socket;

    /**
     * Client Port
     */
    private int m_clientPort;

    /**
     * Is there a connection
     */
    private boolean isConnected = false;

    /**
     *
     * @param clientPort
     */
    public DefaultIlastikHiliteClient(final int clientPort) {
        m_clientPort = clientPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendPositionChangedCommand(final double[] pos, final boolean affectOthers, final boolean clear) {

        // create connection
        establishConnection();

        // Build json document
        String command = "setviewerposition";
        if (clear) {
            command = "unsetviewerposition";
        }

        final JsonObjectBuilder obj = Json.createObjectBuilder().add("command", command);

        // create obj to send to ilastik
        obj.add("x", pos[0]);
        obj.add("y", pos[1]);
        obj.add("z", pos[2]);
        obj.add("c", pos[3]);
        obj.add("t", pos[4]);
        obj.add("name", "knime");
        obj.add("keep", affectOthers);

        // write object to stream
        writeJSONObjectToStream(obj.build());
    }


    /**
     * Writing JsonObject to output stream
     *
     * @param obj
     */
    private void writeJSONObjectToStream(final JsonObject obj) {
        try {
            // get stream
            OutputStream outputStream = m_socket.getOutputStream();

            // open writer
            JsonWriter writer = Json.createWriter(outputStream);

            // write to stream
            writer.writeObject(obj);

            // close writer
            writer.close();
        } catch (final IOException e) {
            System.out.println("No socket available");
        }
    }

    /**
     * Create a new socket if none exists already
     *
     * @return
     */
    private Socket establishConnection() {
        // check if socket already exists
        if (m_socket == null || m_socket.isClosed()) {
            try {
                // create new socket with localhost to client port
                m_socket = new Socket("localhost", m_clientPort);
                isConnected = true;
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                // is Not Connected
                isConnected = false;
                System.out.println("Please start Ilastik server!");
            }
        }

        return m_socket;
    }

    /**
     * On server startup we send this msg
     *
     * @param serverPort
     */
    @Override
    public void sendHandshakeToIlastik(final int serverPort) {
        // make a connection
        establishConnection();

        if (m_socket != null) {
            // create json object to send
            final JsonObjectBuilder obj = Json.createObjectBuilder().add("command", "handshake");

            // from
            obj.add("name", "knime");

            // which port
            obj.add("port", serverPort);

            // write to stream
            writeJSONObjectToStream(obj.build());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeConnection() {
        try {
            // close connection
            if (m_socket != null) {
                m_socket.close();
            }
        } catch (IOException e) {
            System.err.println("IlastikHiliteClient: Error when closing connection");
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConnected() {
        return isConnected;
    }
}
