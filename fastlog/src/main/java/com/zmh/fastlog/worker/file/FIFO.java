package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;

public interface FIFO {
    void put(ByteData byteData);

    ByteData get();

    void next();

    int getFileNum();

    int getTotalFile();
}
