package com.zmh.fastlog.model.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static java.util.Objects.isNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ByteData {
    private long id;
    private byte[] data;
    private int dataLength;

    public int capacity() {
        if (isNull(data)) {
            return 0;
        }
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
