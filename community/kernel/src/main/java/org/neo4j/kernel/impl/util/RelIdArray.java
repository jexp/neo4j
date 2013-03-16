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
package org.neo4j.kernel.impl.util;

import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Enc128;
import org.neo4j.kernel.impl.cache.SizeOf;

/**
 * About the state of the {@link ByteBuffer}s that store the ids in here:
 * 
 * A buffer that is currently assigned to the fields has:
 * + its position where to write new ids.
 * 
 * @author Mattias
 *
 */
public class RelIdArray implements SizeOf
{
    private static final Enc128 enc128 = new Enc128();
    
    private static final DirectionWrapper[] DIRECTIONS_FOR_OUTGOING =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_INCOMING =
            new DirectionWrapper[] { DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_BOTH =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    
    public static class EmptyRelIdArray extends RelIdArray
    {
        private static final DirectionWrapper[] EMPTY_DIRECTION_ARRAY = new DirectionWrapper[0];
        private final RelIdIterator EMPTY_ITERATOR = new RelIdIteratorImpl( this, EMPTY_DIRECTION_ARRAY )
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            protected boolean nextBlock()
            {
                return false;
            }
            
            @Override
            public void doAnotherRound()
            {
            }
            
            @Override
            public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
            {
                return direction.iterator( newSource );
            }
        };
        
        private EmptyRelIdArray( int type )
        {
            super( type );
        }

        @Override
        public RelIdIterator iterator( final DirectionWrapper direction )
        {
            return EMPTY_ITERATOR;
        }
    };
    
    public static RelIdArray empty( int type )
    {
        return new EmptyRelIdArray( type );
    }
    
    public static final RelIdArray EMPTY = new EmptyRelIdArray( -1 );
    
    private final int type;
    private ByteBuffer outIds;
    private ByteBuffer inIds;
    
    public RelIdArray( int type )
    {
        this.type = type;
    }
    
    @Override
    public int size()
    {
        return withObjectOverhead( 8 /*type (padded)*/ + sizeOfBlockWithReference( outIds ) + sizeOfBlockWithReference( inIds ) ); 
    }
    
    static int sizeOfBlockWithReference( ByteBuffer ids )
    {
        return withReference( ids != null ? withObjectOverhead( ids.capacity() ) : 0 );
    }

    public int getType()
    {
        return type;
    }
    
    protected RelIdArray( RelIdArray from )
    {
        this( from.type );
        this.outIds = from.outIds;
        this.inIds = from.inIds;
    }
    
    protected RelIdArray( int type, ByteBuffer out, ByteBuffer in )
    {
        this( type );
        this.outIds = out;
        this.inIds = in;
    }
    
    /*
     * Adding an id with direction BOTH means that it's a loop
     */
    public void add( long id, DirectionWrapper direction )
    {
        ByteBuffer lastBlock = direction.getLastBlock( this );
        if ( lastBlock == null )
        {
            ByteBuffer newLastBlock = null;
            newLastBlock = allocateBuffer( 20 );
            direction.setLastBlock( this, newLastBlock );
            lastBlock = newLastBlock;
        }
        add( lastBlock, id, direction );
    }
    
    private ByteBuffer allocateBuffer( int i )
    {
        ByteBuffer result = ByteBuffer.allocate( i );
        return result;
    }

    private String idsAsString( ByteBuffer buffer )
    {
        ByteBuffer buf = buffer.duplicate();
        buf.position( 0 );
        buf.limit( buffer.position() );
        StringBuilder ids = new StringBuilder();
        int i = 0;
        while ( buf.hasRemaining() )
        {
            ids.append( "," + enc128.decode( buf ) );
            i++;
        }
        return "(" + i + ") " + ids.toString();
    }

    private void add( ByteBuffer buffer, long id, DirectionWrapper direction )
    {
        buffer = ensureBufferSpace( buffer, 5, direction );
        enc128.encode( buffer, id );
    }
    
    private int remainingToWriter( ByteBuffer buffer )
    {
        return buffer.capacity()-buffer.position();
    }
    
    private ByteBuffer ensureBufferSpace( ByteBuffer buffer, int bytesToAdd, DirectionWrapper direction )
    {
        int remaining = remainingToWriter( buffer );
        if ( bytesToAdd > remaining )
        {
            int candidateSize = buffer.capacity()*3;
            if ( candidateSize < remaining+bytesToAdd )
                candidateSize = (remaining+bytesToAdd)*2;
            
            buffer = copy( buffer, candidateSize );
            direction.setLastBlock( this, buffer );
        }
        return buffer;
    }

    private void addAll( ByteBuffer target, ByteBuffer source, DirectionWrapper direction )
    {
        target = ensureBufferSpace( target, source.capacity(), direction );
        target.put( newBufferForReading( source, 0 ) );
    }

    public RelIdArray addAll( RelIdArray source )
    {
        if ( source == null )
        {
            return this;
        }
        
        if ( source.getLastLoopBlock() != null )
        {
            return upgradeIfNeeded( source ).addAll( source );
        }
        
        append( source, DirectionWrapper.OUTGOING );
        append( source, DirectionWrapper.INCOMING );
        append( source, DirectionWrapper.BOTH );
        return this;
    }
    
    protected ByteBuffer getLastLoopBlock()
    {
        return null;
    }
    
    public RelIdArray shrink()
    {
        ByteBuffer shrunkOut = shrink( outIds );
        ByteBuffer shrunkIn = shrink( inIds );
        return shrunkOut == outIds && shrunkIn == inIds ? this : 
                new RelIdArray( type, shrunkOut, shrunkIn );
    }
    
    protected ByteBuffer shrink( ByteBuffer buffer )
    {
        if ( buffer == null || buffer.position() == buffer.capacity() )
            return buffer;
        
        return copy( buffer, buffer.position() );
    }

    private ByteBuffer copy( ByteBuffer buffer, int newSize )
    {
        ByteBuffer newBuffer = allocateBuffer( newSize );
        ByteBuffer reader = buffer.duplicate();
        reader.flip();
        newBuffer.put( reader );
        return newBuffer;
    }
    
    protected void setLastLoopBlock( ByteBuffer block )
    {
        throw new UnsupportedOperationException( "Should've upgraded to RelIdArrayWithLoops before this" );
    }
    
    public RelIdArray upgradeIfNeeded( RelIdArray capabilitiesToMatch )
    {
        return capabilitiesToMatch.getLastLoopBlock() != null ? new RelIdArrayWithLoops( this ) : this;
    }
    
    public RelIdArray downgradeIfPossible()
    {
        return this;
    }
    
    protected void append( RelIdArray source, DirectionWrapper direction )
    {
        ByteBuffer toBlock = direction.getLastBlock( this );
        ByteBuffer fromBlock = direction.getLastBlock( source );
        if ( fromBlock != null )
        {
            if ( toBlock == null )
            {
                direction.setLastBlock( this, copy( fromBlock, fromBlock.position() ) );
            }
            else
            {
                addAll( toBlock, fromBlock, direction );
            }
        }
    }
    
    public boolean isEmpty()
    {
        return outIds == null && inIds == null && getLastLoopBlock() == null;
    }
    
    public RelIdIterator iterator( DirectionWrapper direction )
    {
        return direction.iterator( this );
    }
    
    public RelIdArray newSimilarInstance()
    {
        return new RelIdArray( type );
    }
    
    public static enum DirectionWrapper
    {
        OUTGOING( Direction.OUTGOING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_OUTGOING );
            }

            @Override
            ByteBuffer getLastBlock( RelIdArray ids )
            {
                return ids.outIds;
            }

            @Override
            void setLastBlock( RelIdArray ids, ByteBuffer buffer )
            {
                ids.outIds = buffer;
            }
        },
        INCOMING( Direction.INCOMING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_INCOMING );
            }

            @Override
            ByteBuffer getLastBlock( RelIdArray ids )
            {
                return ids.inIds;
            }

            @Override
            void setLastBlock( RelIdArray ids, ByteBuffer buffer )
            {
                ids.inIds = buffer;
            }
        },
        BOTH( Direction.BOTH )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_BOTH );
            }

            @Override
            ByteBuffer getLastBlock( RelIdArray ids )
            {
                return ids.getLastLoopBlock();
            }

            @Override
            void setLastBlock( RelIdArray ids, ByteBuffer block )
            {
                ids.setLastLoopBlock( block );
            }
        };
        
        private final Direction direction;

        private DirectionWrapper( Direction direction )
        {
            this.direction = direction;
        }
        
        abstract RelIdIterator iterator( RelIdArray ids );
        
        /*
         * Only used during add
         */
        abstract ByteBuffer getLastBlock( RelIdArray ids );
        
        /*
         * Only used during add
         */
        abstract void setLastBlock( RelIdArray ids, ByteBuffer buffer );
        
        public Direction direction()
        {
            return this.direction;
        }
    }
    
    public static DirectionWrapper wrap( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return DirectionWrapper.OUTGOING;
        case INCOMING: return DirectionWrapper.INCOMING;
        case BOTH: return DirectionWrapper.BOTH;
        default: throw new IllegalArgumentException( "" + direction );
        }
    }
    
    private static ByteBuffer newBufferForReading( ByteBuffer buffer, int pos )
    {
        if ( buffer == null )
            return buffer;
        ByteBuffer result = buffer.duplicate();
        result.position( pos );
        result.limit( buffer.position() );
        return result;
    }
    
    public static class RelIdIteratorImpl implements RelIdIterator
    {
        private final DirectionWrapper[] directions;
        private int directionPosition = -1;
        private DirectionWrapper currentDirection;
        private ByteBuffer currentState;
        private final ByteBuffer[] states;
        
        private long nextElement;
        private boolean nextElementDetermined;
        private RelIdArray ids;
        
        RelIdIteratorImpl( RelIdArray ids, DirectionWrapper[] directions )
        {
            this.ids = ids;
            this.directions = directions;
            this.states = new ByteBuffer[directions.length];
            
            // Find the initial block which isn't null. There can be directions
            // which have a null block currently, but could potentially be set
            // after the next getMoreRelationships.
            ByteBuffer block = null;
            while ( block == null && directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                block = currentDirection.getLastBlock( ids );
            }
            
            if ( block != null )
            {
                currentState = newBufferForReading( block, 0 );
                states[directionPosition] = currentState;
            }
        }
        
        @Override
        public int getType()
        {
            return ids.getType();
        }
        
        @Override
        public RelIdArray getIds()
        {
            return ids;
        }
        
        @Override
        public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
        {
            if ( ids != newSource || newSource.couldBeNeedingUpdate() )
            {
                ids = newSource;
                
                // Blocks may have gotten upgraded to support a linked list
                // of blocks, so reestablish those references.
                for ( int i = 0; i < states.length; i++ )
                {
                    if ( states[i] != null )
                    {
                        states[i] = newBufferForReading( directions[i].getLastBlock( ids ), states[i].position() );
                    }
                }
            }
            return this;
        }
        
        @Override
        public boolean hasNext()
        {
            if ( nextElementDetermined )
            {
                return nextElement != -1;
            }
            
            while ( true )
            {
                if ( currentState != null && currentState.hasRemaining() )
                {
                    nextElement = enc128.decode( currentState );
                    nextElementDetermined = true;
                    return true;
                }
                else if ( !nextBlock() )
                    break;
            }
            
            // Keep this false since the next call could come after we've loaded
            // some more relationships
            nextElementDetermined = false;
            nextElement = -1;
            return false;
        }
        
        @Override
        public void doAnotherRound()
        {
            directionPosition = -1;
            nextBlock();
        }

        protected boolean nextBlock()
        {
            while ( directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                ByteBuffer nextState = states[directionPosition];
                if ( nextState != null )
                {
                    currentState = nextState;
                    return true;
                }
                ByteBuffer block = currentDirection.getLastBlock( ids );
                if ( block != null )
                {
                    currentState = newBufferForReading( block, 0 );
                    states[directionPosition] = currentState;
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            nextElementDetermined = false;
            return nextElement;
        }
    }
    
    public static RelIdArray from( RelIdArray src, RelIdArray add, Collection<Long> remove )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                return add.downgradeIfPossible();
            }
            if ( add != null )
            {
                src = src.addAll( add );
                return src.downgradeIfPossible();
            }
            return src;
        }
        else
        {
            if ( src == null && add == null )
            {
                return null;
            }
            RelIdArray newArray = null;
            if ( src != null )
            {
                newArray = src.newSimilarInstance();
                newArray = newArray.addAll( src );
                newArray.removeAll( remove );
            }
            else
            {
                newArray = add.newSimilarInstance();
            }
            if ( add != null )
            {
                newArray = newArray.upgradeIfNeeded( add );
                for ( RelIdIteratorImpl fromIterator = (RelIdIteratorImpl) add.iterator( DirectionWrapper.BOTH ); fromIterator.hasNext();)
                {
                    long value = fromIterator.next();
                    if ( !remove.contains( value ) )
                    {
                        newArray.add( value, fromIterator.currentDirection );
                    }
                }
            }
            return newArray;
        }
    }

    private void removeAll( Collection<Long> excluded )
    {
        removeAll( excluded, DirectionWrapper.OUTGOING );
        removeAll( excluded, DirectionWrapper.INCOMING );
        removeAll( excluded, DirectionWrapper.BOTH );
    }

    private void removeAll( Collection<Long> excluded, DirectionWrapper direction )
    {
        ByteBuffer buffer = direction.getLastBlock( this );
        if ( buffer == null )
            return;
        
        ByteBuffer newBuffer = allocateBuffer( buffer.capacity() );
        for ( RelIdIteratorImpl iterator = (RelIdIteratorImpl) direction.iterator( this ); iterator.hasNext(); )
        {
            long value = iterator.next();
            if ( !excluded.contains( value ) )
                add( newBuffer, value, iterator.currentDirection );
        }
        newBuffer = copy( newBuffer, newBuffer.position() );
        direction.setLastBlock( this, newBuffer );
    }

    /**
     * Optimization in the lazy loading of relationships for a node.
     * {@link RelIdIterator#updateSource(RelIdArray)} is only called if
     * this returns true, i.e if a {@link RelIdArray} or {@link IdBlock} might have
     * gotten upgraded to handle f.ex loops or high id ranges so that the
     * {@link RelIdIterator} gets updated accordingly.
     */
    public boolean couldBeNeedingUpdate()
    {
        return true;
    }
    
    @Override
    public String toString()
    {
        String result = getClass().getSimpleName() + "[type:" + type + "]";
        if ( outIds != null )
            result += "\nout:" + idsAsString( outIds );
        if ( inIds != null )
            result += "\nin:" + idsAsString( inIds );
        if ( getLastLoopBlock() != null )
            result += "\nloop:" + idsAsString( getLastLoopBlock() );
        return result;
    }
}
