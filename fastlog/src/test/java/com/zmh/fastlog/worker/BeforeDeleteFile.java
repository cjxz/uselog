package com.zmh.fastlog.worker;

import org.junit.Before;

import java.io.File;

import static java.util.Objects.isNull;

public class BeforeDeleteFile {

    protected static final String FOLDER = "logs/cache";

    @Before
    public void beforeDelete() {
        deleteFile(new File(FOLDER));
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
