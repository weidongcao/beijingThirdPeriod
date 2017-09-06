package com.rainsoft.design.adapter;

/**
 * 适配器模式作为两个不兼容接口之间的桥梁，这种类型的设计模式属于结构模式
 * 因为该模式组合了两个独立接口。
 * 这种模式涉及一个单独的类，它负责连接独立或不兼容接口的功能。现实生活的例子：
 * 读卡器，其作为为存储卡和笔记本电脑之间的适配器。将存储卡插入读卡器
 * 并将读卡器插入笔记本电脑接口，以便可以通过笔记本电脑读取存储卡
 *
 * 有一个MediaPlayer接口和一个具体的类-AudioPlayer它实现了MediaPlayer接口。
 * 默认情况下音频播放器(AudioPlayer)可以播放器mp3格式的音频文件。
 * 还有另外一个接口AdvancedMediaPlayer和实现AdvancedMediaPlayer接口的具体类。
 * 这些类可以播放vlc和mp4格式的文件。
 * 想让AudioPlayer播放其他格式。要实现这一点需要创建一个适配器类MediaAdapter，
 * 它实现了MediaPlayer接口并使用AdvancedMediaPlayer对象来播放所需的格式。
 * AudioPlayer使用适配器类MediaAdapter它所需的音频类型，而不知道可以播放所需
 * 格式的实际类。AdapterPatternDemo这是一个演示类，它将使用AudioPlayer类来
 * 播放各种格式
 * Created by Administrator on 2017-09-05.
 */
public class AudioPlayer implements MediaPlayer {
    MediaAdapter mediaAdapter;

    @Override
    public void play(String audioType, String fileName) {
        if (audioType.equalsIgnoreCase("mp3")) {
            System.out.println("Playing mp3 file. Name: " + fileName);
        } else if (audioType.equalsIgnoreCase("vlc") || audioType.equalsIgnoreCase("mp4")) {
            mediaAdapter = new MediaAdapter(audioType);
            mediaAdapter.play(audioType, fileName);
        } else {
            System.out.println("Invalid Media. " + audioType + " format not supported");
        }
    }
}
