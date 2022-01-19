package com.zmh.fastlog.model.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ByteData {
    private long id;
    private byte[] data;
    private int dataLength;

    public int capacity() {
        return data.length;
    }
    public void switchData(ByteData byteData) {
        byte[] temp = byteData.getData();

        byteData.setId(id);
        byteData.setData(this.data);
        byteData.setDataLength(dataLength);

        this.data = temp;
    }
}
