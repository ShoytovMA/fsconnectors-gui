import argparse
import asyncio
import datetime
import os
import sys
from typing import Optional

import humanize
from fsconnectors import AsyncLocalConnector, AsyncS3Connector, S3Connector
from fsconnectors.cli import CLI
from PyQt5.QtCore import Qt, QPoint
from PyQt5.QtWidgets import (QAction, QApplication, QFileDialog, QFrame,
                             QHBoxLayout, QInputDialog, QLabel, QLineEdit,
                             QMenu, QMessageBox, QPushButton, QStyle,
                             QTextEdit, QTreeWidget, QTreeWidgetItem,
                             QVBoxLayout, QWidget)


class S3Browser:

    def __init__(self, bucket: str, s3connector: S3Connector):
        self.s3connector = s3connector
        self.bucket = bucket
        self.pwd = f'{self.bucket}/'
        self.app = QApplication(sys.argv)
        self.widget = QWidget()
        self.widget.setWindowTitle('S3Browser')
        self.widget.resize(800, 600)
        self.create_top()
        self.create_body()
        self.create_bottom()
        self.main_layout = QVBoxLayout()
        self.main_layout.addWidget(self.top)
        self.main_layout.addWidget(self.body)
        self.main_layout.addWidget(self.bottom)
        self.widget.setLayout(self.main_layout)
        self.refresh()
        self.widget.show()
        self.app.exec()

    def create_top(self):
        self.top = QFrame(self.widget)
        self.top_layout = QHBoxLayout()
        self.path_label = QLabel('Path: ')
        self.top_layout.addWidget(self.path_label)
        self.path_textbox = QLineEdit(self.pwd)
        self.path_textbox.returnPressed.connect(self.on_button_go)
        self.top_layout.addWidget(self.path_textbox)
        self.path_button = QPushButton('go')
        self.path_button.pressed.connect(self.on_button_go)
        self.top_layout.addWidget(self.path_button)
        self.top.setLayout(self.top_layout)

    def create_body(self):
        self.body = QTreeWidget()
        self.body.setColumnCount(4)
        self.body.setHeaderLabels(['Name', 'Size', 'Type', 'Last modified'])
        self.body.setColumnWidth(0, 400)
        self.body.itemDoubleClicked.connect(self.on_double_click)
        self.body.setContextMenuPolicy(Qt.CustomContextMenu)
        self.body.customContextMenuRequested.connect(self.on_right_click)

    def create_bottom(self):
        self.bottom = QTextEdit()
        self.bottom.setReadOnly(True)

    def refresh(self):
        self.body.clear()
        if not self.pwd.startswith(f'{self.bucket}/'):
            self.pwd = f'{self.bucket}/'
            return self.refresh()
        if self.pwd != f'{self.bucket}/':
            rows = [self.create_body_row('..')]
        else:
            rows = []
        for item in sorted(self.s3connector.scandir(self.pwd), key=lambda x: x.type):
            rows.append(self.create_body_row(item.name, item.size, item.type, item.last_modified))
        self.body.insertTopLevelItems(0, rows)
        self.path_textbox.clear()
        self.path_textbox.insert(self.pwd)

    def create_body_row(
        self,
        name: str,
        size: Optional[int] = None,
        item_type: Optional[str] = None,
        last_modified: Optional[datetime.datetime] = None
    ) -> QTreeWidgetItem:
        size = humanize.naturalsize(size) if size else '--'
        if item_type is None:
            item_type = '--'
            icon = None
        elif item_type == 'dir':
            item_type = 'directory'
            pixmap = QStyle.SP_DirIcon
            icon = self.widget.style().standardIcon(pixmap)
        else:
            item_type = 'file'
            pixmap = QStyle.SP_FileIcon
            icon = self.widget.style().standardIcon(pixmap)
        last_modified = last_modified.strftime('%d.%m.%Y, %H:%M') if last_modified else '--'
        item = QTreeWidgetItem([name, size, item_type, last_modified])
        if icon:
            item.setIcon(0, icon)
        return item

    def on_button_go(self):
        self.pwd = self.path_textbox.text().strip().rstrip('/') + '/'
        if self.pwd == '/':
            self.pwd = f'{self.bucket}/'
        self.refresh()

    def on_double_click(self):
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        item_type = item.data(2, 0)
        if item_name == '..':
            self.pwd = self.pwd.rsplit('/', maxsplit=2)[0] + '/'
            self.refresh()
        elif item_type == 'directory':
            self.pwd += f'{item_name}/'
            self.refresh()
        else:
            self.preview()

    def on_right_click(self, position: QPoint = None):
        menu = QMenu()
        try:
            item = self.body.selectedItems()[0]
            item_type = item.data(2, 0)
        except IndexError:
            return
        if item_type == 'directory':
            action = QAction('Open', menu)
        else:
            action = QAction('Preview', menu)
        action.triggered.connect(self.on_double_click)
        menu.addAction(action)
        action = QAction('Rename', menu)
        action.triggered.connect(self.rename)
        menu.addAction(action)
        action = QAction('Copy', menu)
        action.triggered.connect(self.copy)
        menu.addAction(action)
        action = QAction('Move', menu)
        action.triggered.connect(self.move)
        menu.addAction(action)
        action = QAction('Remove', menu)
        action.triggered.connect(self.remove)
        menu.addAction(action)
        menu.addSeparator()
        action = QAction('Upload', menu)
        action.triggered.connect(self.upload)
        menu.addAction(action)
        action = QAction('Download', menu)
        action.triggered.connect(self.download)
        menu.addAction(action)
        menu.exec_(self.body.viewport().mapToGlobal(position))

    def preview(self):
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        with self.s3connector.open(f'{self.pwd}{item_name}', 'rb') as file:
            text = file.read(1024 * 1024)
            text = text.decode('utf-8', errors='ignore')
        self.bottom.clear()
        self.bottom.setText(text)

    def rename(self):
        rename_message = QInputDialog
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        item_type = item.data(2, 0)
        new_name, ok = rename_message.getText(self.widget, f'Rename {item_type}',
                                              f'Rename {item_name} to:', text=item_name)
        if ok:
            if item_type == 'directory':
                self.s3connector.move(f'{self.pwd}{item_name}/', f'{self.pwd}{new_name}/',
                                      recursive=True)
            else:
                self.s3connector.move(f'{self.pwd}{item_name}', f'{self.pwd}{new_name}')
            self.refresh()

    def copy(self):
        copy_message = QInputDialog
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        item_type = item.data(2, 0)
        dst_path, ok = copy_message.getText(self.widget, f'Copy {item_type}',
                                            f'Copy {item_name} to:', text=self.pwd)
        if ok:
            if not dst_path.startswith(f'{self.bucket}/'):
                dst_path = f'{self.bucket}/{dst_path}'
            dst_path = dst_path.rstrip('/') + '/'
            if item_type == 'directory':
                self.s3connector.copy(f'{self.pwd}{item_name}/', f'{dst_path}{item_name}/',
                                      recursive=True)
            else:
                self.s3connector.copy(f'{self.pwd}{item_name}', f'{dst_path}{item_name}')
            self.refresh()

    def move(self):
        move_message = QInputDialog
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        item_type = item.data(2, 0)
        dst_path, ok = move_message.getText(self.widget, f'Move {item_type}',
                                            f'Move {item_name} to:', text=self.pwd)
        if ok:
            if not dst_path.startswith(f'{self.bucket}/'):
                dst_path = f'{self.bucket}/{dst_path}'
            dst_path = dst_path.rstrip('/') + '/'
            if item_type == 'directory':
                self.s3connector.move(f'{self.pwd}{item_name}/', f'{dst_path}{item_name}/',
                                      recursive=True)
            else:
                self.s3connector.move(f'{self.pwd}{item_name}', f'{dst_path}{item_name}')
            self.refresh()

    def remove(self):
        remove_message = QMessageBox
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        item_type = item.data(2, 0)
        answer = remove_message.question(self.widget, f'Remove {item_type}',
                                         f'Are you sure you want to permanently remove this {item_type}?'
                                         f'\n\n{item_name}',
                                         remove_message.Yes | remove_message.No)
        if answer == remove_message.Yes:
            if item_type == 'directory':
                self.s3connector.remove(f'{self.pwd}{item_name}/', recursive=True)
            else:
                self.s3connector.remove(f'{self.pwd}{item_name}')
            self.refresh()

    def upload(self):
        upload_message = QFileDialog()
        src_path = upload_message.getExistingDirectory()
        src_name = os.path.split(src_path)[-1]
        dst_path = f'{self.pwd}{src_name}/'

        s3_connector = AsyncS3Connector(endpoint_url=self.s3connector.endpoint_url,
                                        aws_access_key_id=self.s3connector.aws_access_key_id,
                                        aws_secret_access_key=self.s3connector.aws_secret_access_key)
        local_connector = AsyncLocalConnector()
        s3util = CLI(s3_connector, local_connector)
        asyncio.run(s3util.upload(local_path=src_path, s3_path=dst_path))
        self.refresh()

    def download(self):
        download_message = QFileDialog()
        item = self.body.selectedItems()[0]
        item_name = item.data(0, 0)
        item_type = item.data(2, 0)
        if item_type == 'directory':
            dst_path = download_message.getExistingDirectory()
            dst_path = f'{dst_path}/{item_name}'
            src_path = f'{self.pwd}{item_name}/'
            s3_connector = AsyncS3Connector(endpoint_url=self.s3connector.endpoint_url,
                                            aws_access_key_id=self.s3connector.aws_access_key_id,
                                            aws_secret_access_key=self.s3connector.aws_secret_access_key)
            local_connector = AsyncLocalConnector()
            s3util = CLI(s3_connector, local_connector)
            asyncio.run(s3util.download(local_path=dst_path, s3_path=src_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True, type=str, help='S3 bucket name')
    parser.add_argument('--config_path', required=True, type=str, help='path to configuration file')
    args = parser.parse_args()

    s3conn = S3Connector.from_yaml(args.config_path)
    browser = S3Browser(bucket=args.bucket, s3connector=s3conn)
