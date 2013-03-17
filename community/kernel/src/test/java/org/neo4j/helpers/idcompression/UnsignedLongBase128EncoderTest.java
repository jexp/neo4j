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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

// MSB -----> LSB
public class UnsignedLongBase128EncoderTest
{
    @Test
    public void shouldEncodeAndDecodeOneByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0, bytes( 0 ) );
        assertEncodeAndDecodeValue( 1, bytes( 1 ) );
        assertEncodeAndDecodeValue( 10, bytes( 10 ) );
        assertEncodeAndDecodeValue( 34, bytes( 34 ) );
        assertEncodeAndDecodeValue( 127, bytes( 127 ) );
    }

    @Test
    public void shouldEncodeAndDecodeTwoByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0b1000_0000, // 128
                             bytes( 0b0000_0001, 0b1000_0000 ) );
        assertEncodeAndDecodeValue( 0b0000_0100____0100_0010, // 1234
                             bytes( 0b0000_1000, 0b1100_0010 ) );
        assertEncodeAndDecodeValue( 0b0011_1111____1111_1111, // 2^15-1
                             bytes( 0b0111_1111, 0b1111_1111 ) );
    }
    
    @Test
    public void shouldEncodeAndDecodeThreeByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0b0001_0110____1011_0001____1011_0010,
                             bytes( 0b0101_1010, 0b1110_0011, 0b1011_0010 ) );
    }
    
    @Test
    public void shouldEncodeAndDecodeEightBytes() throws Exception
    {
        assertEncodeAndDecodeValue( 0b0000_0000__0000_0000__0000_0000__0000_0000__0000_0000__0000_0000__0000_0000__0000_0000L,
                             bytes( 0 ) );
        assertEncodeAndDecodeValue( 0b0000_0000____1000_0001____0000_0010____0000_0100____0000_1000____0001_0000____0010_0000____0100_0000L,
                             bytes( 0b0100_0000, 0b1100_0000, 0b1100_0000, 0b1100_0000, 0b1100_0000, 0b1100_0000, 0b1100_0000, 0b1100_0000 ) );
        assertEncodeAndDecodeValue( 0b0111_1111____1111_1111____1111_1111____1111_1111____1111_1111____1111_1111____1111_1111____1111_1111L,
                bytes( 0b0111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111 ) );
    }
    
    @Test
    public void testPerformance() throws Exception
    {
        int idCount = 10_000_000;
        ByteBuffer buffer = ByteBuffer.allocate( idCount*4 );
        for ( int i = 0; i < 10; i++ )
            timeEncodeDecode( buffer, 10_000_000, 100000 );
    }

    private void timeEncodeDecode( ByteBuffer buffer, int idCount, int mod )
    {
        // ENCODE
        buffer.clear();
        UnsignedLongBase128Encoder encoder = new UnsignedLongBase128Encoder();
        long t = System.currentTimeMillis();
        for ( int i = 0; i < idCount; i++ )
        {
            encoder.encode( buffer, (i%mod)+1 );
        }
        t = System.currentTimeMillis()-t;
        System.out.println( idCount/t + " values/ms " + idCount + " in " + t + "ms " + buffer.position() + " bytes used, avg " + ((float)buffer.position()/idCount) + " bytes/id" );
        
        // DECODE
        buffer.flip();
        t = System.currentTimeMillis();
        for ( int i = 0; i < idCount; i++ )
        {
            encoder.decode( buffer );
        }
        t = System.currentTimeMillis()-t;
        System.out.println( idCount/t + " values/ms " + idCount + " in " + t + "ms" );
    }
    
    private void assertEncodeAndDecodeValue( long value, byte[] expectedEncodedBytes )
    {
        // ENCODE
        byte[] array = new byte[expectedEncodedBytes.length];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        UnsignedLongBase128Encoder encoder = new UnsignedLongBase128Encoder();

        int bytesUsed = encoder.encode( buffer, value );

        assertEquals( expectedEncodedBytes.length, bytesUsed );
        assertArrayEquals( expectedEncodedBytes, array );
        
        // DECODE
        buffer.flip();
        assertEquals( value, encoder.decode( buffer ) );
    }
    
    private byte[] bytes( long... values )
    {
        byte[] result = new byte[values.length];
        for ( int i = 0; i < values.length; i++ )
            result[i] = (byte) values[values.length-i-1];
        return result;
    }

    private byte[] bytes( int... values )
    {
        byte[] result = new byte[values.length];
        for ( int i = 0; i < values.length; i++ )
            result[i] = (byte) values[values.length-i-1];
        return result;
    }
}
