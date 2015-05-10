/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.lhfei.hadoop.ch08;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since May 9, 2015
 */
public class IntPair implements WritableComparable<IntPair> {

	private int first;
	private int second;

	public IntPair() {
	}

	public IntPair(int first, int second) {
		set(first, second);
	}

	public void set(int first, int second) {
		this.first = first;
		this.second = second;
	}

	public int getFirst() {
		return first;
	}

	public int getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public int hashCode() {
		return first * 163 + second;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IntPair) {
			IntPair ip = (IntPair) o;
			return first == ip.first && second == ip.second;
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(IntPair ip) {
		int cmp = compare(first, ip.first);
		if (cmp != 0) {
			return cmp;
		}
		return compare(second, ip.second);
	}

	/**
	 * Convenience method for comparing two ints.
	 */
	public static int compare(int a, int b) {
		return (a < b ? -1 : (a == b ? 0 : 1));
	}

}
