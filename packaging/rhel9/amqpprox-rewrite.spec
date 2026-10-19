%{!?package_version:%global package_version 0.1.0}
%{!?source_revision:%global source_revision local}
%{!?cmake_binary:%global cmake_binary /usr/bin/cmake}
%{!?ctest_binary:%global ctest_binary /usr/bin/ctest}

Name:           amqpprox-rewrite
Version:        %{package_version}
Release:        1.git%{source_revision}%{?dist}
Summary:        AMQP 0.9.1 proxy with scoped legacy compatibility rewriting
License:        Apache-2.0
URL:            https://github.com/xenuser/amqpprox-rewrite
Source0:        %{name}-%{version}.tar.gz
# CMake >= 3.27 is checked by build-rpm.sh; its path can be external to RPM.
BuildRequires:  gcc-c++, make, boost-devel, openssl-devel
BuildRequires:  protobuf-devel, protobuf-compiler, gtest-devel, gmock-devel
BuildRequires:  systemd-rpm-macros
Requires:       python3
Requires(pre):  shadow-utils
%{?systemd_requires}
Conflicts:      amqpprox

%description
Protocol-aware proxy supporting opt-in virtual host rewriting and conversion
of compatible classic queue declarations to quorum declarations. Configure
and validate the service before enabling it. This package does not migrate
existing queues or messages.

%prep
%autosetup
cp packaging/rhel9/README.md RHEL9.md

%build
%{cmake_binary} -S . -B rpm-build -DBUILD_FLAVOUR=system \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=%{_prefix} \
    -DCMAKE_C_FLAGS="%{optflags}" -DCMAKE_CXX_FLAGS="%{optflags}"
%{cmake_binary} --build rpm-build -- %{?_smp_mflags}

%check
%{ctest_binary} --test-dir rpm-build --output-on-failure

%install
DESTDIR=%{buildroot} %{cmake_binary} --install rpm-build --component amqpprox
install -D -m 0644 packaging/rhel9/amqpprox-rewrite.service %{buildroot}%{_unitdir}/amqpprox-rewrite.service
install -D -m 0640 packaging/rhel9/amqpprox-rewrite.sysconfig %{buildroot}%{_sysconfdir}/sysconfig/amqpprox-rewrite
install -D -m 0640 packaging/rhel9/control.conf %{buildroot}%{_sysconfdir}/amqpprox/control.conf
install -D -m 0755 packaging/rhel9/apply-config.py %{buildroot}%{_libexecdir}/amqpprox-apply-config
install -d -m 0750 %{buildroot}%{_localstatedir}/log/amqpprox

%pre
getent group amqpprox >/dev/null || groupadd -r amqpprox
getent passwd amqpprox >/dev/null || useradd -r -g amqpprox -d / -s /sbin/nologin -c 'AMQP proxy' amqpprox

%post
%systemd_post amqpprox-rewrite.service

%preun
%systemd_preun amqpprox-rewrite.service

%postun
%systemd_postun_with_restart amqpprox-rewrite.service

%files
%license LICENSE
%doc README.md docs/legacy-rewrite.md RHEL9.md
%{_bindir}/amqpprox
%{_bindir}/amqpprox_ctl
%{_libexecdir}/amqpprox-apply-config
%{_unitdir}/amqpprox-rewrite.service
%config(noreplace) %attr(0640,root,amqpprox) %{_sysconfdir}/sysconfig/amqpprox-rewrite
%dir %attr(0750,root,amqpprox) %{_sysconfdir}/amqpprox
%config(noreplace) %attr(0640,root,amqpprox) %{_sysconfdir}/amqpprox/control.conf
%dir %attr(0750,amqpprox,amqpprox) %{_localstatedir}/log/amqpprox
