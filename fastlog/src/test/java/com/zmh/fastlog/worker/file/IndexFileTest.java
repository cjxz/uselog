package com.zmh.fastlog.worker.file;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;

public class IndexFileTest {

    @Before
    public void before() {
        deleteFile(new File("logs/cache"));
    }

    @Test
    public void testIndexFile() {
        IndexFile indexFile = new IndexFile(Paths.get("logs/cache"), 1);
        assertEquals(0, indexFile.readIndex(0));
        assertEquals(0, indexFile.writeIndex(0));

        indexFile.write(0, 56);
        assertEquals(56, indexFile.writeIndex(0));

        indexFile.read(0, 8);
        assertEquals(8, indexFile.readIndex(0));
    }

    @Test
    public void testIndexFileOverSize() {
        IndexFile indexFile = new IndexFile(Paths.get("logs/cache"), 2);
        assertEquals(0, indexFile.readIndex(0));
        assertEquals(0, indexFile.writeIndex(0));

        indexFile.write(0, 56);
        assertEquals(56, indexFile.writeIndex(0));

        indexFile.read(0, 8);
        assertEquals(8, indexFile.readIndex(0));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteFile(File file) {
        //判断文件不为null或文件目录存在
        if (isNull(file) || !file.exists()) {
            return;
        }

        //取得这个目录下的所有子文件对象
        File[] files = file.listFiles();
        if (isNull(files)) {
            return;
        }

        //遍历该目录下的文件对象
        for (File f : files) {
            //打印文件名
            String name = f.getName();
            System.out.println("delete:" + name);
            //判断子目录是否存在子目录,如果是文件则删除
            if (f.isDirectory()) {
                deleteFile(f);
            } else {
                f.delete();
            }
        }
    }

}
