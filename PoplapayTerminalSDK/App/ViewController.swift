//
//  ViewController.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 25/01/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import UIKit

class ViewController: UITableViewController {
    
    private var runner: NetworkProxyRunner!
    
    override func viewDidLoad() {
        super.viewDidLoad()
        
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44
        tableView.tableHeaderView = UIView(frame: CGRect(x: 0, y: 0, width: tableView.frame.size.width, height: 20))
        tableView.tableFooterView = UIView()
        tableView.allowsSelection = false
        tableView.register(MessageCell.self, forCellReuseIdentifier: MessageCell.identifier)
        
        let refreshControl = UIRefreshControl()
        refreshControl.addTarget(self, action: #selector(ViewController.refresh), for: .valueChanged)
        self.refreshControl = refreshControl
        
        let center = NotificationCenter.default
        center.addObserver(self, selector: #selector(ViewController.applicationDidBecomeActive(_:)), name: UIApplication.didBecomeActiveNotification, object: nil)
        center.addObserver(self, selector: #selector(ViewController.applicationWillResignActive(_:)), name: UIApplication.willResignActiveNotification, object: nil)
        center.addObserver(self, selector: #selector(ViewController.didReceiveMessage(_:)), name: .jsonRpcConnectionDidReceiveMessage, object: nil)
        center.addObserver(self, selector: #selector(ViewController.didSendMessage(_:)), name: .jsonRpcConnectionDidSendMessage, object: nil)
        
        refresh()
    }
    
    @objc private func refresh() {
        refreshControl?.endRefreshing()
        runner?.stop()
        runner = NetworkProxyRunner()
        runner.start()
    }
    
    @objc private func applicationDidBecomeActive(_ note: Notification) {
    }
    
    @objc private func applicationWillResignActive(_ note: Notification) {
    }
    
    @objc private func didReceiveMessage(_ note: Notification) {
        guard let message = note.object as? JSONObject else { return }
        add(Item(message: message.prettyJsonString(), isReceived: true, timestamp: Date()))
    }
    
    @objc private func didSendMessage(_ note: Notification) {
        guard let message = note.object as? JSONObject else { return }
        add(Item(message: message.prettyJsonString(), isReceived: false, timestamp: Date()))
    }
    
    // MARK: - UITableViewDataSource
    
    private var items: [Item] = []
    
    private func add(_ item: Item) {
        items.insert(item, at: 0)
        tableView.insertRows(at: [IndexPath(row: 0, section: 0)], with: .fade)
    }
    
    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        return items.count
    }
    
    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(withIdentifier: MessageCell.identifier, for: indexPath) as? MessageCell else {
            return UITableViewCell()
        }
        
        let item = items[indexPath.row]
        cell.configure(item: item)
        
        return cell
    }
}

fileprivate struct Item {
    let message: String
    let isReceived: Bool
    let timestamp: Date
}

fileprivate class MessageCell: UITableViewCell {
    
    static let identifier = "MessageCell"
    
    let messageLabel = UILabel()
    let timeLabel = UILabel()
    
    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)
        
        messageLabel.font = UIFont.systemFont(ofSize: 12)
        messageLabel.textColor = UIColor.black
        messageLabel.numberOfLines = 0
        
        timeLabel.font = UIFont.systemFont(ofSize: 12)
        timeLabel.textColor = UIColor.lightGray
        
        contentView.addSubview(messageLabel)
        contentView.addSubview(timeLabel)
        
        messageLabel.translatesAutoresizingMaskIntoConstraints = false
        timeLabel.translatesAutoresizingMaskIntoConstraints = false
        
        NSLayoutConstraint.activate([
            messageLabel.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 10),
            messageLabel.leftAnchor.constraint(equalTo: contentView.leftAnchor, constant: 16),
            messageLabel.rightAnchor.constraint(equalTo: contentView.rightAnchor, constant: -16),
            messageLabel.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -10),
            timeLabel.topAnchor.constraint(equalTo: messageLabel.topAnchor),
            timeLabel.rightAnchor.constraint(equalTo: messageLabel.rightAnchor)
        ])
    }
    
    required init?(coder aDecoder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
    
    func configure(item: Item) {
        messageLabel.text = String(format: "%@: %@", item.isReceived ? "RECV" : "SENT", item.message)
        timeLabel.text = item.timestamp.timeString
    }
}

fileprivate extension Date {
    
    var timeString: String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss.SSS"
        return formatter.string(from: self)
    }
}

fileprivate extension Dictionary {
    
    func prettyJsonString() -> String {
        do {
            let data = try JSONSerialization.data(withJSONObject: self, options: .prettyPrinted)
            return String(data: data, encoding: .utf8)!
        } catch {
            return error.localizedDescription
        }
    }
}
