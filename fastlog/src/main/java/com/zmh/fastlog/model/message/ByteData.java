package com.zmh.fastlog.model.message;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static java.lang.System.arraycopy;
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

    @VisibleForTesting
    public void apply(ByteData byteData) {
        byte[] bytes = new byte[dataLength];

        arraycopy(data, 0, bytes, 0, dataLength);

        byteData.setId(this.getId());
        byteData.setData(bytes);
        byteData.setDataLength(dataLength);
    }
}
