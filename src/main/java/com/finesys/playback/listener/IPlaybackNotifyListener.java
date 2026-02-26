package com.finesys.playback.listener;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/29 14:10</p>
 */
public interface IPlaybackNotifyListener {
    void beforePlayback();
    void finishPlayback();
    void failPlayback();
}
