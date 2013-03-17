/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.helpers.idcompression;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class LinkBlock
{
    private static final int HEADER_SIZE = 8+8+4+4;
    private final UnsignedLongBase128Encoder encoder = new UnsignedLongBase128Encoder();
    private final SignedLongBase128Encoder signedEncoder = new SignedLongBase128Encoder();
    private final Type type;
    private final ByteBuffer buffer;
    private int idCount;
    private long firstRelId, lastRelId;

    public LinkBlock( Type type )
    {
        this.type = type;
        this.buffer = type.allocateBuffer();
    }
    
    public void set( long[][] relAndNodeIdPairs )
    {
        Arrays.sort( relAndNodeIdPairs, SORTER );
        
        // Header
        buffer.putLong( (firstRelId = relAndNodeIdPairs[0][0]) );
        buffer.putLong( (lastRelId = relAndNodeIdPairs[relAndNodeIdPairs.length-1][0]) );
        buffer.putInt( (idCount = relAndNodeIdPairs.length) );
        
        // Ids
        long previousRelId = relAndNodeIdPairs[0][0], previousRelNodeDelta = relAndNodeIdPairs[0][1] - previousRelId;
        encoder.encode( buffer, previousRelId );
        signedEncoder.encode( buffer, previousRelNodeDelta );
        for ( int i = 1; i < relAndNodeIdPairs.length; i++ )
        {
            long[] pair = relAndNodeIdPairs[i];
            long relDelta = pair[0]-previousRelId;
            long relNodeDelta = pair[1]-pair[0];
            long derivativeRelNodeDelta = relNodeDelta-previousRelNodeDelta;
            encoder.encode( buffer, relDelta );
            signedEncoder.encode( buffer, derivativeRelNodeDelta );
            previousRelId = pair[0];
            previousRelNodeDelta = relNodeDelta;
        }
    }
    
    public void get( long[][] target )
    {
        assert target.length >= idCount;
        
        // Header
        buffer.position( HEADER_SIZE );
        
        target[0][0] = encoder.decode( buffer );
        target[0][1] = target[0][0] + signedEncoder.decode( buffer );
        for ( int i = 1; i < target.length; i++ )
        {
            long relDelta = encoder.decode( buffer );
            long relId = target[i-1][0] + relDelta;
            long derivativeRelNodeDelta = signedEncoder.decode( buffer );
            long previousRelNodeDelta = target[i-1][1] - target[i-1][0];
            target[i][0] = relId;
            target[i][1] = previousRelNodeDelta + relId + derivativeRelNodeDelta;
        }
    }
    
    // TODO method for looking up a node for a relationship
    public long getNodeIdForRelId(long targetReldId) {
        if (targetReldId > lastRelId || targetReldId < firstRelId) return -1;

        buffer.position( HEADER_SIZE );
        
        long prevRelId = encoder.decode( buffer );
        long prevNodeId = prevRelId + signedEncoder.decode( buffer );
        if (targetReldId==prevRelId) return prevNodeId;
        for ( int i = 1; i < idCount; i++ )
        {
            long relDelta = encoder.decode( buffer );
            long relId = prevRelId + relDelta;
            long derivativeRelNodeDelta = signedEncoder.decode( buffer );
            long previousRelNodeDelta = prevNodeId - prevRelId;
            prevRelId = relId;
            prevNodeId = previousRelNodeDelta + relId + derivativeRelNodeDelta;
            if (targetReldId==prevRelId) return prevNodeId;
        }
        return -1; // todo throw not found exception?
    }
    
    
    private static final Comparator<long[]> SORTER = new Comparator<long[]>()
    {
        @Override
        public int compare( long[] o1, long[] o2 )
        {
            return Long.compare( o1[0], o2[0] );
        }
    };
    
    public static enum Type
    {
        // TODO think about sizes
        SMALL( 256 ),
        MEDIUM( 256*256 ),
        LARGE( 256*256*256 );
        
        private final int byteSize;

        private Type( int byteSize )
        {
            this.byteSize = byteSize;
        }
        
        int byteSize()
        {
            return byteSize;
        }
        
        ByteBuffer allocateBuffer()
        {
            ByteBuffer result = ByteBuffer.allocate( byteSize );
            result.putInt( byteSize() );
            return result;
        }
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + buffer + "]";
    }
}
