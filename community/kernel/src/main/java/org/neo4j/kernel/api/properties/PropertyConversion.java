/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.lang.reflect.Array;
import java.util.Arrays;

class PropertyConversion
{
    static DefinedProperty convertProperty( int propertyKeyId, Object value )
    {
        if ( value instanceof String )
        {
            return Property.stringProperty( propertyKeyId, (String) value );
        }
        if ( value instanceof Integer )
        {
            return Property.intProperty( propertyKeyId, (Integer) value );
        }
        if ( value instanceof Long )
        {
            return Property.longProperty( propertyKeyId, (Long) value );
        }
        if ( value instanceof Boolean )
        {
            return Property.booleanProperty( propertyKeyId, (Boolean) value );
        }
        if ( value instanceof Object[] )
        {
            return arrayProperty( propertyKeyId, (Object[]) value );
        }
        if ( value instanceof Double )
        {
            return Property.doubleProperty( propertyKeyId, (Double) value );
        }
        if ( value instanceof Float )
        {
            return Property.floatProperty( propertyKeyId, (Float) value );
        }
        if ( value instanceof Short )
        {
            return Property.shortProperty( propertyKeyId, (Short) value );
        }
        if ( value instanceof Byte )
        {
            return Property.byteProperty( propertyKeyId, (Byte) value );
        }
        if ( value instanceof Character )
        {
            return Property.charProperty( propertyKeyId, (Character) value );
        }
        if ( value instanceof byte[] )
        {
            return Property.byteArrayProperty( propertyKeyId, ((byte[]) value).clone() );
        }
        if ( value instanceof long[] )
        {
            return Property.longArrayProperty( propertyKeyId, ((long[]) value).clone() );
        }
        if ( value instanceof int[] )
        {
            return Property.intArrayProperty( propertyKeyId, ((int[]) value).clone() );
        }
        if ( value instanceof double[] )
        {
            return Property.doubleArrayProperty( propertyKeyId, ((double[]) value).clone() );
        }
        if ( value instanceof float[] )
        {
            return Property.floatArrayProperty( propertyKeyId, ((float[]) value).clone() );
        }
        if ( value instanceof boolean[] )
        {
            return Property.booleanArrayProperty( propertyKeyId, ((boolean[]) value).clone() );
        }
        if ( value instanceof char[] )
        {
            return Property.charArrayProperty( propertyKeyId, ((char[]) value).clone() );
        }
        if ( value instanceof short[] )
        {
            return Property.shortArrayProperty( propertyKeyId, ((short[]) value).clone() );
        }
        // otherwise fail
        if ( value == null )
        {
            throw new IllegalArgumentException( "[null] is not a supported property value" );
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format( "[%s:%s] is not a supported property value", value, value.getClass().getName() ) );
        }
    }

    private static DefinedProperty arrayProperty( int propertyKeyId, Object[] value )
    {
        if ( value instanceof String[] )
        {
            return Property.stringArrayProperty( propertyKeyId, checkNull(
                    (String[]) value.clone() ) );
        }
        if ( value instanceof Integer[] )
        {
            return Property.intArrayProperty( propertyKeyId,
                    copy( (Integer[]) value ) );
        }
        if ( value instanceof Boolean[] )
        {
            return Property.booleanArrayProperty( propertyKeyId,
                    copy( (Boolean[])value ) );
        }
        if ( value instanceof Long[] )
        {
            return Property.longArrayProperty( propertyKeyId,
                    copy( (Long[]) value ) );
        }
        if ( value instanceof Byte[] )
        {
            return Property.byteArrayProperty( propertyKeyId,
                    copy( (Byte[]) value ) );
        }
        if ( value instanceof Double[] )
        {
            return Property.doubleArrayProperty( propertyKeyId,
                    copy( (Double[])value ) );
        }
        if ( value instanceof Float[] )
        {
            return Property.floatArrayProperty( propertyKeyId,
                    copy( (Float[])value ) );
        }
        if ( value instanceof Character[] )
        {
            return Property.charArrayProperty( propertyKeyId,
                    copy( (Character[])value ) );
        }
        if ( value instanceof Short[] )
        {
            return Property.shortArrayProperty( propertyKeyId,
                    copy( (Short[])value ) );
        }
        throw new IllegalArgumentException(
                String.format( "%s[] is not a supported property value type",
                               value.getClass().getComponentType().getName() ) );
    }

    private static <T> T[] checkNull( T[] values )
    {
        for ( T value : values )
        {
            checkNull( value );
        }
        return values;
    }

    private static <T> T checkNull( T t )
    {
        if ( t == null )
        {
            throw new IllegalArgumentException( "Property array value elements may not be null." );
        }
        return t;
    }

    private static byte[] copy( Byte[] value )
    {
        int size = value.length;
        byte[] target = new byte[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static long[] copy( Long[] value )
    {
        int size = value.length;
        long[] target = new long[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static int[] copy( Integer[] value )
    {
        int size = value.length;
        int[] target = new int[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static boolean[] copy( Boolean[] value )
    {
        int size = value.length;
        boolean[] target = new boolean[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static short[] copy( Short[] value )
    {
        int size = value.length;
        short[] target = new short[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static double[] copy( Double[] value )
    {
        int size = value.length;
        double[] target = new double[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static float[] copy( Float[] value )
    {
        int size = value.length;
        float[] target = new float[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
    private static char[] copy( Character[] value )
    {
        int size = value.length;
        char[] target = new char[size];
        for ( int i = 0; i < size; i++ )
        {
            target[i] = checkNull( value[i] );
        }
        return target;
    }
}
