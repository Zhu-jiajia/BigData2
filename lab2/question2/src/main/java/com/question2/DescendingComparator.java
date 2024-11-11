package com.question2;

import org.apache.hadoop.io.DoubleWritable;

public class DescendingComparator extends DoubleWritable.Comparator {

    // 对字节数组的比较
    @Override
    public int compare(byte[] first, int start1, int length1, byte[] second, int start2, int length2) {
        // 调用父类的比较方法并反转结果
        int result = super.compare(first, start1, length1, second, start2, length2);
        return -result; // 返回反向结果，达到降序效果
    }

    // 对对象的比较
    @Override
    public int compare(Object o1, Object o2) {
        // 调用父类的比较方法并反转结果
        int result = super.compare(o1, o2);
        return -result; // 返回反向结果，达到降序效果
    }
}
